"""Versioned credentials storage for parallel-cli.

Stores per-org credentials (main API key + control-API tokens) under a single
file with a ``version`` field so the schema can evolve. v0 files (the flat
``{"access_token": ...}`` shape) are migrated to v1 in place on first load.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path

CREDENTIALS_FILE = Path.home() / ".config" / "parallel-web-tools" / "credentials.json"
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
    control_api: ControlApiTokens = field(default_factory=ControlApiTokens)


@dataclass
class Credentials:
    version: int = CURRENT_VERSION
    selected_org_id: str | None = None
    orgs: dict[str, OrgCredentials] = field(default_factory=dict)

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
    )


def load() -> Credentials | None:
    """Load credentials from disk, migrating v0 → v1 in place if needed.

    Returns ``None`` if the file doesn't exist or is unreadable/corrupt.
    """
    if not CREDENTIALS_FILE.exists():
        return None
    try:
        with open(CREDENTIALS_FILE) as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None

    migrated = "version" not in raw
    if migrated:
        raw = _migrate_v0(raw)

    creds = _credentials_from_dict(raw)

    if migrated:
        save(creds)

    return creds


def save(creds: Credentials) -> None:
    """Write credentials atomically with 0o600 permissions."""
    CREDENTIALS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(creds)
    # Write to a temp file in the same directory, then atomically rename.
    fd, tmp_path = tempfile.mkstemp(
        prefix=".credentials.",
        suffix=".tmp",
        dir=str(CREDENTIALS_FILE.parent),
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2)
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, CREDENTIALS_FILE)
    except Exception:
        if os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        raise


def delete() -> bool:
    """Remove the credentials file. Returns True if a file was removed."""
    if CREDENTIALS_FILE.exists():
        CREDENTIALS_FILE.unlink()
        return True
    return False


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
