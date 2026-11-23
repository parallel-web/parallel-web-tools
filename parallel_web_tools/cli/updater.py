"""Auto-update checker for standalone CLI."""

import json
import sys
import time
from pathlib import Path

# Update check interval (24 hours)
UPDATE_CHECK_INTERVAL = 86400

# Config directory
CONFIG_DIR = Path.home() / ".parallel-cli"
CONFIG_FILE = CONFIG_DIR / "config.json"
UPDATE_STATE_FILE = CONFIG_DIR / "update-state.json"

# Set once this process installed an update, so it is not installed twice.
_updated_this_process = False


def _load_json_file(path: Path) -> dict:
    """Load JSON from file, returning empty dict on any error."""
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def _save_json_file(path: Path, data: dict, indent: int | None = None) -> None:
    """Save JSON to file, creating parent directories as needed."""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=indent))


def is_auto_update_check_enabled() -> bool:
    """Check if auto-update check is enabled in config (default: True)."""
    return _load_json_file(CONFIG_FILE).get("auto_update_check", True)


def set_auto_update_check(enabled: bool) -> None:
    """Enable or disable auto-update check."""
    config = _load_json_file(CONFIG_FILE)
    config["auto_update_check"] = enabled
    _save_json_file(CONFIG_FILE, config, indent=2)


def should_check_for_updates() -> bool:
    """Check if we should check for updates now.

    Returns True if running in standalone mode, auto-update is enabled,
    and enough time has passed since last check.
    """
    if not getattr(sys, "frozen", False):
        return False
    if not is_auto_update_check_enabled():
        return False
    last_check = _load_json_file(UPDATE_STATE_FILE).get("last_check", 0)
    return (time.time() - last_check) > UPDATE_CHECK_INTERVAL


def _is_newer_version(latest: str, current: str) -> bool:
    """Check if latest version is newer than current version."""
    from packaging.version import Version

    try:
        return Version(latest) > Version(current)
    except Exception:
        # If version parsing fails, only return True if they differ
        # and latest looks like a higher version (simple heuristic)
        if latest == current:
            return False
        # Fall back to string comparison - not perfect but safe
        # This will return False for most edge cases, avoiding false positives
        return False


def _fetch_latest_release(timeout: int = 10) -> dict | None:
    """Fetch latest release info from GitHub. Returns None on error."""
    import httpx

    try:
        resp = httpx.get(
            "https://api.github.com/repos/parallel-web/parallel-web-tools/releases/latest",
            timeout=timeout,
            follow_redirects=True,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def check_for_update_notification(current_version: str, save_state: bool = True) -> str | None:
    """Check for updates and return notification message if available.

    Args:
        current_version: The current CLI version
        save_state: If True, updates the last_check timestamp (for automatic checks).
                   Set to False for explicit --check calls to avoid resetting the timer.

    Returns None if no update available or on error.
    This is designed to be fast and non-blocking.
    """
    # Update last check time (so we don't spam on errors)
    if save_state:
        _save_json_file(UPDATE_STATE_FILE, {"last_check": time.time()})

    release = _fetch_latest_release(timeout=5)
    if not release:
        return None

    latest_version = release["tag_name"].lstrip("v")
    if not _is_newer_version(latest_version, current_version):
        return None

    return f"Update available: v{current_version} → v{latest_version}"


def _mark_updated() -> None:
    global _updated_this_process
    _updated_this_process = True


def get_platform() -> str | None:
    """Get platform string for downloads."""
    import platform

    system = platform.system().lower()
    machine = platform.machine().lower()

    if system == "darwin":
        return "darwin-arm64" if machine == "arm64" else "darwin-x64"
    elif system == "linux":
        return "linux-arm64" if machine in ("arm64", "aarch64") else "linux-x64"
    elif system == "windows":
        return "windows-x64"
    return None


def download_and_install_update(current_version: str, console, force: bool = False) -> bool:
    """Download and install the latest update.

    Args:
        current_version: The current CLI version
        console: Rich console for output
        force: If True, reinstall even if already at latest version

    Returns True on success, False on failure.
    """
    import hashlib
    import platform
    import shutil
    import tempfile
    import zipfile

    import httpx

    plat = get_platform()
    if not plat:
        console.print("[red]Unsupported platform[/red]")
        return False

    console.print("[dim]Checking for updates...[/dim]")
    release = _fetch_latest_release(timeout=10)
    if not release:
        console.print("[red]Failed to check for updates[/red]")
        return False

    latest_version = release["tag_name"].lstrip("v")

    if not _is_newer_version(latest_version, current_version) and not force:
        console.print(f"[green]Already up to date (v{current_version})[/green]")
        return True

    if force and not _is_newer_version(latest_version, current_version):
        console.print(f"[cyan]Reinstalling v{latest_version}...[/cyan]")
    else:
        console.print(f"[cyan]Updating: v{current_version} → v{latest_version}[/cyan]")

    # Find the asset for our platform
    archive_name = f"parallel-cli-{plat}.zip"
    checksum_name = f"{archive_name}.sha256"

    archive_url = None
    checksum_url = None
    for asset in release["assets"]:
        if asset["name"] == archive_name:
            archive_url = asset["browser_download_url"]
        elif asset["name"] == checksum_name:
            checksum_url = asset["browser_download_url"]

    if not archive_url:
        console.print(f"[red]No release found for platform: {plat}[/red]")
        return False

    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            archive_path = tmp_path / archive_name

            # Download archive
            console.print("[dim]Downloading...[/dim]")
            with httpx.stream("GET", archive_url, timeout=120, follow_redirects=True) as resp:
                resp.raise_for_status()
                with open(archive_path, "wb") as f:
                    for chunk in resp.iter_bytes():
                        f.write(chunk)

            # Verify checksum if available
            if checksum_url:
                try:
                    resp = httpx.get(checksum_url, timeout=10, follow_redirects=True)
                    expected_checksum = resp.text.strip()

                    with open(archive_path, "rb") as f:
                        actual_checksum = hashlib.sha256(f.read()).hexdigest()

                    if actual_checksum != expected_checksum:
                        console.print("[red]Checksum verification failed[/red]")
                        return False
                except Exception:
                    console.print("[yellow]Warning: Could not verify checksum[/yellow]")

            # Extract archive
            console.print("[dim]Installing...[/dim]")
            extract_dir = tmp_path / "extracted"
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(extract_dir)

            # Find the current executable location (resolve symlinks so we
            # update the real install dir, not e.g. ~/.local/bin/)
            current_exe = Path(sys.executable).resolve()
            install_dir = current_exe.parent

            # The archive contains a parallel-cli folder
            new_cli_dir = extract_dir / "parallel-cli"
            if not new_cli_dir.exists():
                console.print("[red]Invalid archive structure[/red]")
                return False

            # Replace the installation
            system = platform.system().lower()
            if system == "windows":
                # Rename current to .old, copy new
                backup_dir = install_dir.parent / "parallel-cli.old"
                if backup_dir.exists():
                    shutil.rmtree(backup_dir)
                shutil.move(str(install_dir), str(backup_dir))
                shutil.copytree(str(new_cli_dir), str(install_dir))
                console.print(f"[green]Updated to v{latest_version}[/green]")
                console.print("[dim]Please restart the CLI to use the new version[/dim]")
                _mark_updated()
            else:
                # On Unix, we can replace files while running
                for item in new_cli_dir.iterdir():
                    dest = install_dir / item.name
                    if dest.exists():
                        if dest.is_dir():
                            shutil.rmtree(dest)
                        else:
                            dest.unlink()
                    if item.is_dir():
                        shutil.copytree(str(item), str(dest))
                    else:
                        shutil.copy2(str(item), str(dest))

                # zipfile.extractall() doesn't preserve Unix permissions, so we
                # need to set the executable bit (equivalent to chmod +x)
                main_exe = install_dir / "parallel-cli"
                if main_exe.exists():
                    main_exe.chmod(main_exe.stat().st_mode | 0o111)

                console.print(f"[green]Updated to v{latest_version}[/green]")
                _mark_updated()

            return True

    except Exception as e:
        console.print(f"[red]Update failed: {e}[/red]")
        return False


# Deprecation-triggered updates bypass the daily interval but are still
# rate-limited, so repeated commands don't hit the GitHub API every time.
DEPRECATION_CHECK_INTERVAL = 3600


def detect_install_method(executable: str | None = None, frozen: bool | None = None) -> str:
    """Return how this CLI was installed: standalone, homebrew, npm, uv-tool, or pip."""
    exe = (executable or sys.executable).replace("\\", "/")
    try:
        exe = str(Path(exe).resolve()).replace("\\", "/")
    except Exception:
        pass
    is_frozen = getattr(sys, "frozen", False) if frozen is None else frozen
    if "/Caskroom/" in exe or "/Cellar/" in exe:
        return "homebrew"
    if "/node_modules/" in exe:
        return "npm"
    if is_frozen:
        return "standalone"
    if "/uv/tools/" in exe:
        return "uv-tool"
    return "pip"


UPDATE_COMMANDS = {
    "standalone": "parallel-cli update",
    "homebrew": "brew upgrade --cask parallel-cli",
    "npm": "npm install -g parallel-web-cli@latest",
    "uv-tool": "uv tool upgrade parallel-web-tools",
    "pip": "pip install --upgrade parallel-web-tools",
}


def _deprecation_check_due() -> bool:
    last = _load_json_file(UPDATE_STATE_FILE).get("last_deprecation_check", 0)
    return (time.time() - last) > DEPRECATION_CHECK_INTERVAL


def _save_deprecation_check(latest_version: str | None) -> None:
    state = _load_json_file(UPDATE_STATE_FILE)
    state["last_deprecation_check"] = time.time()
    if latest_version:
        state["latest_version"] = latest_version
    _save_json_file(UPDATE_STATE_FILE, state)


def _latest_version_for_deprecation() -> str | None:
    """Latest release version, from GitHub at most once per interval, else from saved state."""
    if not _deprecation_check_due():
        return _load_json_file(UPDATE_STATE_FILE).get("latest_version")
    release = _fetch_latest_release(timeout=5)
    latest = release["tag_name"].lstrip("v") if release and release.get("tag_name") else None
    _save_deprecation_check(latest)
    return latest


def _describe_notice(notice) -> str:
    parts = [notice.message or f"{notice.endpoint or 'An API endpoint'} is deprecated."]
    if notice.successor:
        parts.append(f"Replacement: {notice.successor}")
    if notice.link:
        parts.append(f"Docs: {notice.link}")
    if notice.sunset:
        parts.append(f"Sunset: {notice.sunset}")
    return " ".join(parts)


def handle_deprecation_notices(current_version: str, notices: list, console) -> str | None:
    """React to deprecated API calls made by this CLI run.

    Standalone installs with auto-update enabled update themselves when a newer
    release exists. In every other case, print a notice on ``console`` (stderr)
    that tells the user or the invoking agent how to update.

    Returns "updated", "notified", or None when there was nothing to do.
    """
    global _updated_this_process
    if not notices:
        return None

    for notice in notices:
        console.print(f"[yellow][deprecated] {_describe_notice(notice)}[/yellow]", highlight=False)

    method = detect_install_method()
    try:
        latest = _latest_version_for_deprecation()
    except Exception:
        latest = None
    newer_available = bool(latest) and _is_newer_version(latest, current_version)

    if _updated_this_process:
        return "updated"

    if newer_available and method == "standalone" and is_auto_update_check_enabled():
        console.print("[dim]This parallel-cli version calls deprecated Parallel APIs. Updating...[/dim]")
        try:
            if download_and_install_update(current_version, console):
                _updated_this_process = True
                console.print("[dim]Run the command again to use the new version.[/dim]")
                return "updated"
        except Exception:
            pass

    command = UPDATE_COMMANDS[method]
    if newer_available:
        console.print(
            f"[yellow][parallel-cli] UPDATE REQUIRED: parallel-cli v{current_version} calls deprecated "
            f"Parallel APIs. v{latest} is available. Update with: {command}[/yellow]",
            highlight=False,
        )
    else:
        console.print(
            f"[yellow][parallel-cli] parallel-cli v{current_version} calls deprecated Parallel APIs. "
            f"Update when a newer release is available: {command}[/yellow]",
            highlight=False,
        )
    return "notified"
