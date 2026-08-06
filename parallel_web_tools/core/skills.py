"""Skill installation helpers for parallel-cli."""

from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import httpx

DEFAULT_SKILLS_INDEX_URL = "https://skills.parallel.ai/index.json"
SKILLS_INDEX_URL_ENV = "PARALLEL_SKILLS_INDEX_URL"
DEFAULT_SKILLS_REPO_REF = "main"
SKILLS_REPO_REF_ENV = "PARALLEL_SKILLS_REPO_REF"
GLOBAL_SKILLS_DIR_ENV = "PARALLEL_SKILLS_GLOBAL_DIR"

PROJECT_ROOT_MARKERS = (".git", "pyproject.toml", "package.json")
MANIFEST_FILE_NAME = ".parallel-cli-skills-manifest.json"

# Claude Code only scans its own tree, so installed skills are mirrored into it.
CLAUDE_CODE_DIR_NAME = ".claude"


class SkillsError(Exception):
    """Base error for skills operations."""


class SkillsInstallLocationError(SkillsError):
    """Raised when a project-local install directory cannot be determined."""


class SkillsDownloadError(SkillsError):
    """Raised when remote skills metadata or files cannot be fetched."""


class SkillsInputError(SkillsError):
    """Raised when caller-provided skill arguments are invalid."""


def get_skills_repo_ref() -> str:
    """Return the legacy requested skills channel/ref override.

    CDN-backed installs ignore this value and always use the channel advertised by
    the remote index, but we keep the helper for backwards compatibility.
    """
    configured = os.environ.get(SKILLS_REPO_REF_ENV)
    if configured and configured.strip():
        return configured.strip()
    return DEFAULT_SKILLS_REPO_REF


def get_skills_index_url() -> str:
    """Return the CDN index URL used for skills downloads."""
    configured = os.environ.get(SKILLS_INDEX_URL_ENV)
    if configured and configured.strip():
        return configured.strip()
    return DEFAULT_SKILLS_INDEX_URL


def get_global_skills_dir() -> Path:
    """Return the global skills directory path."""
    configured = os.environ.get(GLOBAL_SKILLS_DIR_ENV)
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".agents" / "skills"


def find_project_root(start: Path | None = None) -> Path | None:
    """Find a project root by walking upward for known root markers."""
    cursor = (start or Path.cwd()).resolve()
    for candidate in (cursor, *cursor.parents):
        for marker in PROJECT_ROOT_MARKERS:
            if (candidate / marker).exists():
                return candidate
    return None


def resolve_install_dir(project: bool, start: Path | None = None) -> Path:
    """Resolve install directory for global or project-local skills."""
    if not project:
        return get_global_skills_dir()

    root = find_project_root(start=start)
    if root is None:
        raise SkillsInstallLocationError(
            "Could not determine project root from current directory. "
            "Run this inside a project containing one of: .git, pyproject.toml, package.json."
        )
    return root / ".agents" / "skills"


def get_claude_code_skills_dir(project_root: Path | None = None) -> Path:
    """Return the directory Claude Code scans for skills.

    Global skills live under ``~/.claude/skills``; project skills live under
    ``<project_root>/.claude/skills``. This mirrors ``resolve_install_dir`` but
    targets Claude Code's tree instead of the canonical ``.agents/skills`` tree.
    """
    base = project_root if project_root is not None else Path.home()
    return Path(base) / CLAUDE_CODE_DIR_NAME / "skills"


def link_into_claude_code(
    install_dir: Path,
    skill_names: list[str],
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Expose already-installed skills to Claude Code.

    Skills are installed into the canonical ``.agents/skills`` tree, which Gemini
    CLI, Copilot, Codex and Amp read but Claude Code does not, so freshly
    installed skills are invisible to it. When Claude Code is present (a
    ``.claude`` directory exists), link each installed skill folder into
    ``<...>/.claude/skills`` with an absolute-target symlink, falling back to a
    copy where symlinks are unavailable (e.g. Windows without the required
    privilege). Links left over from skills that are no longer installed are
    pruned, so narrowing the managed set does not leave dead commands behind.

    This is best-effort: any failure is captured in the returned ``warnings`` and
    never raised, so it cannot break the primary ``.agents/skills`` install.
    """
    result: dict[str, Any] = {
        "claude_dir_present": False,
        "claude_skills_dir": None,
        "linked": [],
        "copied": [],
        "skipped": [],
        "pruned": [],
        "warnings": [],
    }

    install_dir = Path(install_dir)
    present = [path for path in (install_dir / name for name in skill_names) if path.is_dir()]

    try:
        claude_root = (project_root if project_root is not None else Path.home()) / CLAUDE_CODE_DIR_NAME
        if not claude_root.exists():
            # Claude Code is not installed here; nothing to expose.
            return result

        result["claude_dir_present"] = True
        claude_skills_dir = get_claude_code_skills_dir(project_root)
        result["claude_skills_dir"] = str(claude_skills_dir)

        if _resolves_to_same_dir(claude_skills_dir, install_dir):
            # The whole skills dir is already a symlink to the install dir
            # (either ours or set up by hand); per-skill links would be redundant.
            result["skipped"] = [path.name for path in present]
            return result

        for source in present:
            name = source.name
            target = source.resolve()
            claude_skills_dir.mkdir(parents=True, exist_ok=True)
            link_path = claude_skills_dir / name

            if link_path.is_symlink():
                if _resolves_to_same_dir(link_path, target):
                    # Idempotent: correct link already in place.
                    result["skipped"].append(name)
                    continue
                # A managed symlink pointing at the wrong target: refresh it.
                link_path.unlink()
            elif link_path.exists():
                # A real file/dir with this name already exists; do not clobber it.
                result["skipped"].append(name)
                result["warnings"].append(f"Left existing '{link_path}' untouched (not a parallel-cli symlink).")
                continue

            try:
                os.symlink(target, link_path, target_is_directory=True)
                result["linked"].append(name)
            except (OSError, NotImplementedError):
                # Symlinks unsupported/unprivileged (e.g. Windows): copy instead.
                try:
                    shutil.copytree(target, link_path)
                    result["copied"].append(name)
                except OSError as copy_error:
                    result["warnings"].append(f"Could not expose '{name}' to Claude Code: {copy_error}")

        result["pruned"] = _prune_dead_links(claude_skills_dir, install_dir, result["warnings"])
    except OSError as error:
        result["warnings"].append(f"Skipped Claude Code integration: {error}")

    return result


def _prune_dead_links(claude_skills_dir: Path, install_dir: Path, warnings: list[str]) -> list[str]:
    """Remove links to skills that no longer exist in ``install_dir``.

    Installing a narrower ``--skill`` set removes the skills dropped from the
    managed set, which would otherwise leave dangling links (and dead slash
    commands) behind in Claude Code's tree.
    """
    pruned: list[str] = []
    if not claude_skills_dir.is_dir():
        return pruned

    install_root = install_dir.resolve()
    for entry in sorted(claude_skills_dir.iterdir()):
        # Only dangling symlinks that we could have created are candidates.
        if not entry.is_symlink() or entry.exists():
            continue
        try:
            target = Path(os.readlink(entry))
        except OSError:
            continue
        if target.parent != install_root:
            continue
        try:
            entry.unlink()
            pruned.append(entry.name)
        except OSError as error:
            warnings.append(f"Could not remove stale link '{entry}': {error}")

    return pruned


def unlink_from_claude_code(
    install_dir: Path,
    skill_names: list[str],
    project_root: Path | None = None,
) -> dict[str, Any]:
    """Remove Claude Code links previously created by ``link_into_claude_code``.

    Only symlinks pointing into ``install_dir`` are removed, so a user's own
    skill of the same name is never deleted. Copies made by the Windows fallback
    are indistinguishable from hand-authored skills, so they are reported for
    manual cleanup instead of being removed.
    """
    result: dict[str, Any] = {
        "claude_skills_dir": None,
        "unlinked": [],
        "skipped": [],
        "warnings": [],
    }

    install_dir = Path(install_dir)

    try:
        claude_skills_dir = get_claude_code_skills_dir(project_root)
        if not claude_skills_dir.exists():
            return result

        result["claude_skills_dir"] = str(claude_skills_dir)
        if _resolves_to_same_dir(claude_skills_dir, install_dir):
            # The skills dir itself points at the install dir; nothing per-skill to undo.
            return result

        for name in skill_names:
            link_path = claude_skills_dir / name
            if not link_path.is_symlink():
                if link_path.exists():
                    result["skipped"].append(name)
                continue

            if not _resolves_to_same_dir(link_path, install_dir / name):
                # Points somewhere else entirely; leave it alone.
                result["skipped"].append(name)
                continue

            try:
                link_path.unlink()
                result["unlinked"].append(name)
            except OSError as error:
                result["warnings"].append(f"Could not remove '{link_path}': {error}")

        # Catch links for skills that predate the manifest we just cleared.
        result["unlinked"].extend(_prune_dead_links(claude_skills_dir, install_dir, result["warnings"]))
    except OSError as error:
        result["warnings"].append(f"Skipped Claude Code cleanup: {error}")

    return result


def _resolves_to_same_dir(candidate: Path, expected: Path) -> bool:
    """Return whether two paths resolve to the same location.

    Both paths may be dangling symlinks or not exist at all (``resolve`` is
    non-strict), which is the case when comparing links whose target has just
    been uninstalled.
    """
    try:
        return candidate.resolve() == Path(expected).resolve()
    except OSError:
        return False


@contextmanager
def _skills_client() -> Iterator[httpx.Client]:
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        yield client


def _fetch_json(client: httpx.Client, url: str, description: str) -> dict[str, Any]:
    response = client.get(url)
    if response.status_code >= 400:
        raise SkillsDownloadError(f"Failed to download {description} from {url}: HTTP {response.status_code}")

    try:
        data = response.json()
    except ValueError as e:
        raise SkillsDownloadError(f"Failed to parse {description} from {url} as JSON") from e

    if not isinstance(data, dict):
        raise SkillsDownloadError(f"Expected {description} at {url} to be a JSON object")
    return data


def _fetch_skills_index(client: httpx.Client) -> dict[str, Any]:
    return _fetch_json(client, get_skills_index_url(), "skills index")


def _index_channel(index: dict[str, Any]) -> str:
    channel = index.get("channel")
    if isinstance(channel, str) and channel.strip():
        return channel.strip()
    return DEFAULT_SKILLS_REPO_REF


def _skills_from_index(index: dict[str, Any]) -> dict[str, dict[str, str]]:
    raw_skills = index.get("skills")
    if not isinstance(raw_skills, list):
        raise SkillsDownloadError("Skills index is missing a valid 'skills' list")

    parsed: dict[str, dict[str, str]] = {}
    for raw_skill in raw_skills:
        if not isinstance(raw_skill, dict):
            raise SkillsDownloadError("Skills index contained an invalid skill entry")

        name = raw_skill.get("name")
        skill_url = raw_skill.get("skill_url")
        if not isinstance(name, str) or not name.strip():
            raise SkillsDownloadError("Skills index contained a skill with an invalid name")
        if not isinstance(skill_url, str) or not skill_url.strip():
            raise SkillsDownloadError(f"Skills index entry '{name}' is missing a valid skill_url")

        parsed[name.strip()] = {
            "name": name.strip(),
            "skill_url": skill_url.strip(),
        }

    return parsed


def _list_skills_from_index(index: dict[str, Any]) -> list[str]:
    return sorted(_skills_from_index(index))


def _download_skill_markdown(client: httpx.Client, skill_name: str, skill_url: str) -> bytes:
    response = client.get(skill_url)
    if response.status_code >= 400:
        raise SkillsDownloadError(
            f"Failed to download skill '{skill_name}' from {skill_url}: HTTP {response.status_code}"
        )
    return response.content


def get_remote_skills_channel() -> str:
    """Return the channel advertised by the remote CDN index."""
    with _skills_client() as client:
        index = _fetch_skills_index(client)
    return _index_channel(index)


def list_remote_skills(ref: str | None = None) -> list[str]:
    """Return available skill names from the CDN index.

    The ref argument is ignored for CDN-backed installs.
    """
    del ref
    with _skills_client() as client:
        index = _fetch_skills_index(client)
    return _list_skills_from_index(index)


def _manifest_path(install_dir: Path) -> Path:
    return install_dir / MANIFEST_FILE_NAME


def _write_manifest(install_dir: Path, ref: str, installed_skills: list[str]) -> None:
    data = {
        "source": get_skills_index_url(),
        "ref": ref,
        "installed_skills": sorted(installed_skills),
        "installed_at": int(time.time()),
        "managed_by": "parallel-cli",
    }
    install_dir.mkdir(parents=True, exist_ok=True)
    _manifest_path(install_dir).write_text(json.dumps(data, indent=2))


def _read_manifest(install_dir: Path) -> dict:
    path = _manifest_path(install_dir)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def install_skills(
    install_dir: Path,
    selected_skills: list[str] | None = None,
    ref: str | None = None,
) -> dict:
    """Install selected (or all) skills into install_dir.

    Only skills previously managed by parallel-cli are reconciled. Unmanaged skill
    directories are left untouched.
    """
    del ref

    with _skills_client() as client:
        index = _fetch_skills_index(client)
        resolved_ref = _index_channel(index)
        available_skills = _skills_from_index(index)
        available = sorted(available_skills)
        requested = sorted(set(selected_skills or available))
        missing = sorted(name for name in requested if name not in available_skills)
        if missing:
            raise SkillsInputError(
                f"Unknown skills requested: {', '.join(missing)}. Available skills: {', '.join(available)}"
            )

        manifest = _read_manifest(install_dir)
        managed_raw = manifest.get("installed_skills")
        previously_managed: list[str] = (
            [name for name in managed_raw if isinstance(name, str)] if isinstance(managed_raw, list) else []
        )

        install_dir.mkdir(parents=True, exist_ok=True)

        for skill_name in previously_managed:
            if skill_name not in requested:
                skill_dir = install_dir / skill_name
                if skill_dir.exists() and skill_dir.is_dir():
                    shutil.rmtree(skill_dir)

        for skill_name in requested:
            skill_dir = install_dir / skill_name
            if skill_dir.exists():
                shutil.rmtree(skill_dir)
            skill_dir.mkdir(parents=True, exist_ok=True)
            skill_bytes = _download_skill_markdown(client, skill_name, available_skills[skill_name]["skill_url"])
            (skill_dir / "SKILL.md").write_bytes(skill_bytes)

    _write_manifest(install_dir, resolved_ref, requested)
    return {
        "install_dir": str(install_dir),
        "ref": resolved_ref,
        "installed_skills": requested,
        "count": len(requested),
    }


def uninstall_skills(install_dir: Path) -> dict:
    """Uninstall only manifest-managed skills from install_dir."""
    manifest = _read_manifest(install_dir)
    managed_raw = manifest.get("installed_skills")
    managed: list[str] = (
        [name for name in managed_raw if isinstance(name, str)] if isinstance(managed_raw, list) else []
    )
    removed: list[str] = []

    for skill_name in managed:
        skill_path = install_dir / skill_name
        if skill_path.exists() and skill_path.is_dir():
            shutil.rmtree(skill_path)
            removed.append(skill_name)

    manifest_path = _manifest_path(install_dir)
    if manifest_path.exists():
        manifest_path.unlink()

    return {
        "install_dir": str(install_dir),
        "removed_skills": sorted(removed),
        "count": len(removed),
    }


def reinstall_skills(
    install_dir: Path,
    selected_skills: list[str] | None = None,
    ref: str | None = None,
) -> dict:
    """Reinstall skills by uninstalling managed set then installing fresh."""
    uninstall_result = uninstall_skills(install_dir)
    install_result = install_skills(install_dir, selected_skills=selected_skills, ref=ref)
    return {
        "install_dir": install_result["install_dir"],
        "ref": install_result["ref"],
        "removed_skills": uninstall_result["removed_skills"],
        "installed_skills": install_result["installed_skills"],
        "removed_count": uninstall_result["count"],
        "installed_count": install_result["count"],
    }
