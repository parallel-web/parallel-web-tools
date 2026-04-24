"""Skill installation helpers for parallel-cli.

Manages downloading skills from the ``parallel-agent-skills`` GitHub repo into
local ``.agents/skills`` directories.
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import time
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
    """Resolve install directory for global or project-local skills.

    Global installs use ``~/.agents/skills`` by default.
    Project installs use ``<project-root>/.agents/skills`` and fail if a root
    marker cannot be found.
    """
    if not project:
        return get_global_skills_dir()

    root = find_project_root(start=start)
    if root is None:
        raise SkillsInstallLocationError(
            "Could not determine project root from current directory. "
            "Run this inside a project containing one of: .git, pyproject.toml, package.json."
        )
    return root / ".agents" / "skills"


def _github_contents_url(path: str, ref: str) -> str:
    encoded_path = quote(path, safe="/")
    encoded_ref = quote(ref, safe="")
    return (
        f"https://api.github.com/repos/{SKILLS_REPO_OWNER}/{SKILLS_REPO_NAME}/contents/{encoded_path}?ref={encoded_ref}"
    )


def _read_json_response(response: httpx.Response) -> dict | list:
    try:
        return response.json()
    except Exception as e:
        raise SkillsDownloadError("Failed to decode GitHub response as JSON") from e


def _github_headers() -> dict[str, str]:
    """Build GitHub API headers, using GH_TOKEN when available."""
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    token = os.environ.get(GITHUB_TOKEN_ENV)
    if token and token.strip():
        headers["Authorization"] = f"Bearer {token.strip()}"
    return headers


def _get_contents(client: httpx.Client, path: str, ref: str) -> list[dict]:
    url = _github_contents_url(path, ref)
    response = client.get(url)
    if response.status_code >= 400:
        raise SkillsDownloadError(
            f"Failed to fetch GitHub contents for {path} at ref '{ref}' in "
            f"{SKILLS_REPO_OWNER}/{SKILLS_REPO_NAME}: HTTP {response.status_code}"
        )
    payload = _read_json_response(response)
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    raise SkillsDownloadError(f"Unexpected GitHub payload for {path}")


def list_remote_skills(ref: str | None = None) -> list[str]:
    """Return available skill directory names from the remote repository."""
    resolved_ref = ref or get_skills_repo_ref()
    with httpx.Client(timeout=30, follow_redirects=True, headers=_github_headers()) as client:
        skills = _list_remote_skills_with_client(client, resolved_ref)
    return skills


def _list_remote_skills_with_client(client: httpx.Client, ref: str) -> list[str]:
    entries = _get_contents(client, SKILLS_REPO_SKILLS_PATH, ref)
    skills = [item["name"] for item in entries if item.get("type") == "dir" and isinstance(item.get("name"), str)]
    return sorted(skills)


def _download_dir(client: httpx.Client, remote_path: str, dest: Path, ref: str) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    entries = _get_contents(client, remote_path, ref)

    for item in entries:
        item_type = item.get("type")
        item_name = item.get("name")
        item_path = item.get("path")
        if not isinstance(item_name, str) or not isinstance(item_path, str):
            continue

        local_path = dest / item_name

        if item_type == "dir":
            _download_dir(client, item_path, local_path, ref)
            continue

        if item_type != "file":
            continue

        # Use the contents API URL (already anchored to the requested ref)
        # instead of raw download_url to avoid silently reading default-branch
        # content when testing non-main refs.
        file_url = item.get("url")
        if not isinstance(file_url, str) or not file_url:
            raise SkillsDownloadError(f"Missing contents URL for {item_path}")

        file_resp = client.get(file_url)
        if file_resp.status_code >= 400:
            raise SkillsDownloadError(f"Failed to download {item_path}: HTTP {file_resp.status_code}")

        file_payload = _read_json_response(file_resp)
        if not isinstance(file_payload, dict):
            raise SkillsDownloadError(f"Unexpected payload while downloading {item_path}")

        encoding = file_payload.get("encoding")
        content = file_payload.get("content")
        if encoding != "base64" or not isinstance(content, str):
            raise SkillsDownloadError(f"Missing base64 content for {item_path}")

        try:
            file_bytes = base64.b64decode(content.replace("\n", ""))
        except Exception as e:
            raise SkillsDownloadError(f"Failed to decode file content for {item_path}") from e

        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(file_bytes)


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
    """Install selected (or all) skills into ``install_dir``."""
    resolved_ref = ref or get_skills_repo_ref()

    with httpx.Client(timeout=30, follow_redirects=True, headers=_github_headers()) as client:
        available = _list_remote_skills_with_client(client, resolved_ref)
        requested = sorted(set(selected_skills or available))
        missing = sorted([name for name in requested if name not in available])
        if missing:
            raise SkillsError(
                f"Unknown skills requested: {', '.join(missing)}. Available skills: {', '.join(available)}"
            )

        install_dir.mkdir(parents=True, exist_ok=True)
        for skill_name in requested:
            skill_dir = install_dir / skill_name
            if skill_dir.exists():
                shutil.rmtree(skill_dir)
            _download_dir(client, f"{SKILLS_REPO_SKILLS_PATH}/{skill_name}", skill_dir, resolved_ref)

    _write_manifest(install_dir, resolved_ref, requested)
    return {
        "install_dir": str(install_dir),
        "ref": resolved_ref,
        "installed_skills": requested,
        "count": len(requested),
    }


def uninstall_skills(install_dir: Path) -> dict:
    """Uninstall only manifest-managed skills from ``install_dir``."""
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
