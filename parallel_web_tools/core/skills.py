"""Skill installation helpers for parallel-cli."""

from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import time
import zipfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

import httpx

SKILLS_REPO_OWNER = "parallel-web"
SKILLS_REPO_NAME = "parallel-agent-skills"
SKILLS_REPO_SKILLS_PATH = "skills"
DEFAULT_SKILLS_REPO_REF = "main"
SKILLS_REPO_REF_ENV = "PARALLEL_SKILLS_REPO_REF"
GITHUB_TOKEN_ENV = "GH_TOKEN"
GLOBAL_SKILLS_DIR_ENV = "PARALLEL_SKILLS_GLOBAL_DIR"

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
    """Return repository ref used for skill downloads."""
    configured = os.environ.get(SKILLS_REPO_REF_ENV)
    if configured and configured.strip():
        return configured.strip()
    return DEFAULT_SKILLS_REPO_REF


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


def _github_archive_url(ref: str) -> str:
    encoded_ref = quote(ref, safe="")
    return f"https://api.github.com/repos/{SKILLS_REPO_OWNER}/{SKILLS_REPO_NAME}/zipball/{encoded_ref}"


def _github_headers() -> dict[str, str]:
    """Build GitHub API headers for skills archive downloads."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get(GITHUB_TOKEN_ENV)
    if token and token.strip():
        headers["Authorization"] = f"Bearer {token.strip()}"
    return headers


def _download_repo_archive(client: httpx.Client, ref: str) -> bytes:
    # TODO: add retry/backoff for transient GitHub API failures (429/5xx).
    response = client.get(_github_archive_url(ref))
    if response.status_code >= 400:
        raise SkillsDownloadError(
            f"Failed to download skills archive at ref '{ref}' from "
            f"{SKILLS_REPO_OWNER}/{SKILLS_REPO_NAME}: HTTP {response.status_code}"
        )
    return response.content


def _extract_repo_archive(archive_bytes: bytes, dest_dir: Path) -> Path:
    """Extract a GitHub zipball into dest_dir and return the archive root."""
    dest_dir.mkdir(parents=True, exist_ok=True)

    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
            root_name: str | None = None

            for member in zf.infolist():
                member_path = Path(member.filename)
                parts = member_path.parts
                if not parts:
                    continue
                if parts[0] in ("", "/"):
                    raise SkillsDownloadError("Invalid archive entry path")
                if any(part == ".." for part in parts):
                    raise SkillsDownloadError("Archive contains unsafe path traversal entry")
                if root_name is None:
                    root_name = parts[0]

                target = dest_dir / member_path
                target_resolved = target.resolve()
                dest_resolved = dest_dir.resolve()
                if dest_resolved not in (target_resolved, *target_resolved.parents):
                    raise SkillsDownloadError("Archive extraction would escape destination directory")

                if member.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue

                target.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as src, target.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
    except zipfile.BadZipFile as e:
        raise SkillsDownloadError("Failed to read downloaded skills archive") from e

    if not root_name:
        raise SkillsDownloadError("Downloaded skills archive was empty")

    root = dest_dir / root_name
    if not root.exists() or not root.is_dir():
        raise SkillsDownloadError("Downloaded skills archive had no repository root directory")
    return root


@contextmanager
def _downloaded_repo_root(ref: str) -> Iterator[Path]:
    with httpx.Client(timeout=30, follow_redirects=True, headers=_github_headers()) as client:
        archive_bytes = _download_repo_archive(client, ref)

    with tempfile.TemporaryDirectory(prefix="parallel-skills-") as tmpdir:
        repo_root = _extract_repo_archive(archive_bytes, Path(tmpdir))
        yield repo_root


def _skills_root(repo_root: Path) -> Path:
    skills_root = repo_root / SKILLS_REPO_SKILLS_PATH
    if not skills_root.exists() or not skills_root.is_dir():
        raise SkillsDownloadError(
            f"Downloaded repository does not contain a '{SKILLS_REPO_SKILLS_PATH}/' directory at the requested ref"
        )
    return skills_root


def _list_skills_from_repo_root(repo_root: Path) -> list[str]:
    skills_root = _skills_root(repo_root)
    return sorted(path.name for path in skills_root.iterdir() if path.is_dir())


def list_remote_skills(ref: str | None = None) -> list[str]:
    """Return available skill directory names from the remote repository."""
    resolved_ref = ref or get_skills_repo_ref()
    with _downloaded_repo_root(resolved_ref) as repo_root:
        return _list_skills_from_repo_root(repo_root)


def _manifest_path(install_dir: Path) -> Path:
    return install_dir / MANIFEST_FILE_NAME


def _write_manifest(install_dir: Path, ref: str, installed_skills: list[str]) -> None:
    data = {
        "repo": f"{SKILLS_REPO_OWNER}/{SKILLS_REPO_NAME}",
        "skills_path": SKILLS_REPO_SKILLS_PATH,
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
    resolved_ref = ref or get_skills_repo_ref()

    with _downloaded_repo_root(resolved_ref) as repo_root:
        skills_root = _skills_root(repo_root)
        available = _list_skills_from_repo_root(repo_root)
        requested = sorted(set(selected_skills or available))
        missing = sorted(name for name in requested if name not in available)
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
            shutil.copytree(skills_root / skill_name, skill_dir)

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
