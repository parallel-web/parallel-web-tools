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

    return f"Update available: v{current_version} → v{latest_version}. Run `parallel-cli update` to install."


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

            # Find the current executable location
            current_exe = Path(sys.executable)
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

                console.print(f"[green]Updated to v{latest_version}[/green]")

            return True

    except Exception as e:
        console.print(f"[red]Update failed: {e}[/red]")
        return False
