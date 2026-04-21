"""Versioned credentials storage for parallel-cli.

New structured auth state lives in ``auth.json``. The legacy flat
``credentials.json`` file is left in its old shape for backward compatibility
with older CLI releases.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

AUTH_FILE = Path.home() / ".config" / "parallel-web-tools" / "auth.json"
LEGACY_CREDENTIALS_FILE = Path.home() / ".config" / "parallel-web-tools" / "credentials.json"
# Backward-compatible alias used across the codebase/tests for the new auth file.
CREDENTIALS_FILE = AUTH_FILE
CURRENT_VERSION = 1
LEGACY_ORG_ID = "legacy"


@dataclass
class ControlApiTokens:
    access_token: str | None = None
    access_token_expires_at: int | None = None
    access_token_scopes: list[str] = field(default_factory=list)
    refresh_token: str | None = None
    refresh_token_expires_at: int | None = None
    authorization_expires_at: int | None = None


@dataclass
class OrgCredentials:
    api_key: str | None = None
    org_name: str | None = None
    control_api: ControlApiTokens = field(default_factory=ControlApiTokens)


@dataclass
class Credentials:
    version: int = CURRENT_VERSION
    selected_org_id: str | None = None
    orgs: dict[str, OrgCredentials] = field(default_factory=dict)
    # Dynamically-registered OAuth client_id returned by
    # ``/getServiceKeys/register``. ``None`` means registration hasn't
    # succeeded yet (first boot, migrated v0 file, or prior failure) — the
    # next login attempt will retry and fall back to a hardcoded id on error.
    client_id: str | None = None

    def selected_org(self) -> OrgCredentials | None:
        if self.selected_org_id is None:
            return None
        return self.orgs.get(self.selected_org_id)


def _migrate_v0(raw: dict) -> dict:
    """Transform a v0 credentials dict into v1 shape.

    v0 shape: ``{"access_token": "<token>"}`` — a single API key with no org context.
    The token is wrapped into a placeholder ``legacy`` org so existing users keep
    working without re-authenticating.
    """
    legacy_token = raw.get("access_token")
    org: dict = {}
    if legacy_token:
        org["api_key"] = legacy_token
    return {
        "version": CURRENT_VERSION,
        "selected_org_id": LEGACY_ORG_ID if legacy_token else None,
        "orgs": {LEGACY_ORG_ID: org} if legacy_token else {},
    }


def _credentials_from_dict(data: dict) -> Credentials:
    orgs_raw = data.get("orgs") or {}
    orgs: dict[str, OrgCredentials] = {}
    for org_id, org_data in orgs_raw.items():
        control_raw = (org_data or {}).get("control_api") or {}
        orgs[org_id] = OrgCredentials(
            api_key=(org_data or {}).get("api_key"),
            org_name=(org_data or {}).get("org_name"),
            control_api=ControlApiTokens(
                access_token=control_raw.get("access_token"),
                access_token_expires_at=control_raw.get("access_token_expires_at"),
                access_token_scopes=list(control_raw.get("access_token_scopes") or []),
                refresh_token=control_raw.get("refresh_token"),
                refresh_token_expires_at=control_raw.get("refresh_token_expires_at"),
                authorization_expires_at=control_raw.get("authorization_expires_at"),
            ),
        )
    return Credentials(
        version=data.get("version", CURRENT_VERSION),
        selected_org_id=data.get("selected_org_id"),
        orgs=orgs,
        client_id=data.get("client_id"),
    )


def _load_json_file(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def _write_json_file(path: Path, payload: dict, temp_prefix: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=temp_prefix,
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise


def load() -> Credentials | None:
    """Load credentials from disk.

    Preference order:
    1. ``auth.json`` in the new structured format.
    2. Legacy ``credentials.json`` in the flat v0 format, migrated forward into
       ``auth.json`` when the new file does not exist.

    Returns ``None`` if neither file exists or both are unreadable/corrupt.
    """
    raw = _load_json_file(CREDENTIALS_FILE)
    if raw is not None:
        if "version" not in raw:
            return _credentials_from_dict(_migrate_v0(raw))
        return _credentials_from_dict(raw)

    legacy_raw = _load_json_file(LEGACY_CREDENTIALS_FILE)
    if legacy_raw is None:
        return None
    creds = _credentials_from_dict(_migrate_v0(legacy_raw))
    save(creds)
    return creds


def save(creds: Credentials) -> None:
    """Write structured auth atomically to ``auth.json``."""
    payload = asdict(creds)
    _write_json_file(CREDENTIALS_FILE, payload, ".auth.")


def delete() -> bool:
    """Remove auth files. Returns True if any local auth file was removed."""
    removed = False
    for path in (CREDENTIALS_FILE, LEGACY_CREDENTIALS_FILE):
        if path.exists():
            path.unlink()
            removed = True
    return removed


def get_active_credentials_file() -> Path | None:
    """Return the on-disk auth file currently backing ``load()``, if any."""
    raw = _load_json_file(CREDENTIALS_FILE)
    if raw is not None:
        return CREDENTIALS_FILE
    legacy_raw = _load_json_file(LEGACY_CREDENTIALS_FILE)
    if legacy_raw is not None:
        return LEGACY_CREDENTIALS_FILE
    return None


def get_selected_api_key() -> str | None:
    """Return the API key for the currently selected org, or None."""
    creds = load()
    if creds is None:
        return None
    org = creds.selected_org()
    if org is None:
        return None
    return org.api_key


def set_api_key_for_org(org_id: str, api_key: str) -> None:
    """Write an API key into the given org, creating the org if missing.

    If no org is currently selected, ``selected_org_id`` is set to ``org_id``.
    """
    creds = load() or Credentials()
    org = creds.orgs.get(org_id) or OrgCredentials()
    org.api_key = api_key
    creds.orgs[org_id] = org
    if creds.selected_org_id is None:
        creds.selected_org_id = org_id
    save(creds)
