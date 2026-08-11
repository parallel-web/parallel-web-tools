"""Skill installation helpers for parallel-cli."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any

import httpx

DEFAULT_SKILLS_INDEX_URL = "https://skills.parallel.ai/index.json"
SKILLS_INDEX_URL_ENV = "PARALLEL_SKILLS_INDEX_URL"
DEFAULT_SKILLS_REPO_REF = "main"
SKILLS_REPO_REF_ENV = "PARALLEL_SKILLS_REPO_REF"
GLOBAL_SKILLS_DIR_ENV = "PARALLEL_SKILLS_GLOBAL_DIR"
CLAUDE_CONFIG_DIR_ENV = "CLAUDE_CONFIG_DIR"

PROJECT_ROOT_MARKERS = (".git", "pyproject.toml", "package.json")
MANIFEST_FILE_NAME = ".parallel-cli-skills-manifest.json"


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


def get_claude_config_dir() -> Path:
    """Return the Claude Code configuration directory."""
    configured = os.environ.get(CLAUDE_CONFIG_DIR_ENV)
    if configured and configured.strip():
        return Path(configured.strip()).expanduser()
    return Path.home() / ".claude"


def find_project_root(start: Path | None = None) -> Path | None:
    """Find a project root by walking upward for known root markers."""
    cursor = (start or Path.cwd()).resolve()
    for candidate in (cursor, *cursor.parents):
        for marker in PROJECT_ROOT_MARKERS:
            if (candidate / marker).exists():
                return candidate
    return None


def resolve_install_dir(project: bool, start: Path | None = None) -> Path:
    """Resolve the canonical ``.agents/skills`` install directory."""
    if not project:
        return get_global_skills_dir()

    root = find_project_root(start=start)
    if root is None:
        raise SkillsInstallLocationError(
            "Could not determine project root from current directory. "
            "Run this inside a project containing one of: .git, pyproject.toml, package.json."
        )
    return root / ".agents" / "skills"


def resolve_install_dirs(project: bool, start: Path | None = None) -> list[Path]:
    """Resolve every directory skills should be installed into.

    ``.agents/skills`` is the canonical cross-agent location and always comes first.
    Claude Code does not read it — it only discovers skills under ``.claude/skills`` —
    so when a Claude Code configuration directory is present we install there too.
    Agents that read both (Cursor, for example) de-duplicate by skill name.

    An explicit ``PARALLEL_SKILLS_GLOBAL_DIR`` override targets exactly one directory,
    on the assumption that a caller naming a path wants only that path written.
    """
    canonical = resolve_install_dir(project=project, start=start)
    if not project and os.environ.get(GLOBAL_SKILLS_DIR_ENV):
        return [canonical]

    claude_config_dir = canonical.parent.parent / ".claude" if project else get_claude_config_dir()
    if not claude_config_dir.is_dir():
        return [canonical]

    return _dedupe_dirs([canonical, claude_config_dir / "skills"])


def _dedupe_dirs(dirs: Iterable[Path]) -> list[Path]:
    """Drop directories that resolve to the same location, preserving order.

    A user who symlinked ``~/.claude/skills`` at ``~/.agents/skills`` would otherwise
    have the same tree installed, and its manifest rewritten, twice.
    """
    seen: set[Path] = set()
    unique: list[Path] = []
    for directory in dirs:
        resolved = Path(directory).expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(Path(directory))
    return unique


def _normalize_install_dirs(install_dirs: Path | str | Iterable[Path | str]) -> list[Path]:
    """Accept a single directory or a collection of them and return a clean list."""
    if isinstance(install_dirs, (str, Path)):
        candidates: list[Path | str] = [install_dirs]
    else:
        candidates = list(install_dirs)

    if not candidates:
        raise SkillsInstallLocationError("No skills install directory was provided.")

    return _dedupe_dirs(Path(candidate) for candidate in candidates)


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

        entry = {
            "name": name.strip(),
            "skill_url": skill_url.strip(),
        }

        manifest_url = raw_skill.get("manifest_url")
        if isinstance(manifest_url, str) and manifest_url.strip():
            entry["manifest_url"] = manifest_url.strip()

        parsed[name.strip()] = entry

    return parsed


def _list_skills_from_index(index: dict[str, Any]) -> list[str]:
    return sorted(_skills_from_index(index))


def _download_skill_file(client: httpx.Client, skill_name: str, skill_url: str) -> bytes:
    response = client.get(skill_url)
    if response.status_code >= 400:
        raise SkillsDownloadError(
            f"Failed to download skill '{skill_name}' from {skill_url}: HTTP {response.status_code}"
        )
    return response.content


def _safe_skill_file_path(skill_name: str, raw_path: str) -> str:
    """Validate a manifest-declared relative path stays inside the skill directory."""
    candidate = raw_path.strip().replace("\\", "/")
    if not candidate:
        raise SkillsDownloadError(f"Manifest for skill '{skill_name}' contained an empty file path")

    posix_path = PurePosixPath(candidate)
    if posix_path.is_absolute() or any(part in ("..", "") for part in posix_path.parts):
        raise SkillsDownloadError(f"Manifest for skill '{skill_name}' contained an unsafe file path: {raw_path}")

    return str(posix_path)


def _resolve_skill_files(client: httpx.Client, skill_name: str, entry: dict[str, str]) -> list[dict[str, str]]:
    """Resolve every file belonging to a skill.

    Indexes that advertise a ``manifest_url`` carry the full file list (references,
    scripts, bundled agents). Older or custom indexes without one fall back to the
    single ``SKILL.md`` document.
    """
    skill_only = [{"path": "SKILL.md", "url": entry["skill_url"], "sha256": ""}]

    manifest_url = entry.get("manifest_url")
    if not manifest_url:
        return skill_only

    manifest = _fetch_json(client, manifest_url, f"manifest for skill '{skill_name}'")
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list) or not raw_files:
        return skill_only

    resolved: list[dict[str, str]] = []
    for raw_file in raw_files:
        if not isinstance(raw_file, dict):
            raise SkillsDownloadError(f"Manifest for skill '{skill_name}' contained an invalid file entry")

        raw_path = raw_file.get("path")
        file_url = raw_file.get("url")
        if not isinstance(raw_path, str) or not isinstance(file_url, str) or not file_url.strip():
            raise SkillsDownloadError(f"Manifest for skill '{skill_name}' contained a file entry missing path or url")

        checksum = raw_file.get("sha256")
        resolved.append(
            {
                "path": _safe_skill_file_path(skill_name, raw_path),
                "url": file_url.strip(),
                "sha256": checksum.strip().lower() if isinstance(checksum, str) else "",
            }
        )

    if not any(file_entry["path"] == "SKILL.md" for file_entry in resolved):
        raise SkillsDownloadError(f"Manifest for skill '{skill_name}' does not include a SKILL.md entry")

    return resolved


def _verify_skill_file_checksum(skill_name: str, file_entry: dict[str, str], content: bytes) -> None:
    expected = file_entry.get("sha256")
    if not expected:
        return

    actual = hashlib.sha256(content).hexdigest()
    if actual != expected:
        raise SkillsDownloadError(
            f"Checksum mismatch for '{skill_name}/{file_entry['path']}': expected {expected}, got {actual}"
        )


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


def _managed_skills(install_dir: Path) -> list[str]:
    """Return the skill names parallel-cli previously installed into install_dir."""
    managed_raw = _read_manifest(install_dir).get("installed_skills")
    if not isinstance(managed_raw, list):
        return []
    return [name for name in managed_raw if isinstance(name, str)]


def install_skills(
    install_dirs: Path | str | Iterable[Path | str],
    selected_skills: list[str] | None = None,
    ref: str | None = None,
) -> dict:
    """Install selected (or all) skills into every directory in install_dirs.

    Only skills previously managed by parallel-cli are reconciled. Unmanaged skill
    directories are left untouched. Every file is downloaded once and written to each
    directory, so a mid-download failure leaves no location partially installed.
    """
    del ref

    targets = _normalize_install_dirs(install_dirs)

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

        downloads: dict[str, list[tuple[str, bytes]]] = {}
        for skill_name in requested:
            skill_files = _resolve_skill_files(client, skill_name, available_skills[skill_name])
            payload: list[tuple[str, bytes]] = []
            for file_entry in skill_files:
                content = _download_skill_file(client, skill_name, file_entry["url"])
                _verify_skill_file_checksum(skill_name, file_entry, content)
                payload.append((file_entry["path"], content))
            downloads[skill_name] = payload

    file_count = 0
    for install_dir in targets:
        previously_managed = _managed_skills(install_dir)
        install_dir.mkdir(parents=True, exist_ok=True)

        for skill_name in previously_managed:
            if skill_name not in requested:
                stale_dir = install_dir / skill_name
                if stale_dir.exists() and stale_dir.is_dir():
                    shutil.rmtree(stale_dir)

        for skill_name, payload in downloads.items():
            skill_dir = install_dir / skill_name
            if skill_dir.exists():
                shutil.rmtree(skill_dir)
            skill_dir.mkdir(parents=True, exist_ok=True)

            for relative_path, content in payload:
                target = skill_dir / relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)

            file_count += len(payload)

        _write_manifest(install_dir, resolved_ref, requested)

    return {
        "install_dirs": [str(directory) for directory in targets],
        "ref": resolved_ref,
        "installed_skills": requested,
        "count": len(requested),
        "file_count": file_count,
    }


def uninstall_skills(install_dirs: Path | str | Iterable[Path | str]) -> dict:
    """Uninstall only manifest-managed skills from every directory in install_dirs."""
    targets = _normalize_install_dirs(install_dirs)
    removed: set[str] = set()

    for install_dir in targets:
        for skill_name in _managed_skills(install_dir):
            skill_path = install_dir / skill_name
            if skill_path.exists() and skill_path.is_dir():
                shutil.rmtree(skill_path)
                removed.add(skill_name)

        manifest_path = _manifest_path(install_dir)
        if manifest_path.exists():
            manifest_path.unlink()

    return {
        "install_dirs": [str(directory) for directory in targets],
        "removed_skills": sorted(removed),
        "count": len(removed),
    }


def reinstall_skills(
    install_dirs: Path | str | Iterable[Path | str],
    selected_skills: list[str] | None = None,
    ref: str | None = None,
) -> dict:
    """Reinstall skills by uninstalling managed set then installing fresh."""
    uninstall_result = uninstall_skills(install_dirs)
    install_result = install_skills(install_dirs, selected_skills=selected_skills, ref=ref)
    return {
        "install_dirs": install_result["install_dirs"],
        "ref": install_result["ref"],
        "removed_skills": uninstall_result["removed_skills"],
        "installed_skills": install_result["installed_skills"],
        "removed_count": uninstall_result["count"],
        "installed_count": install_result["count"],
        "file_count": install_result["file_count"],
    }
