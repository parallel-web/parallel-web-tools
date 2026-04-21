"""Tests for the versioned credentials storage module."""

import json
import os

import pytest

from parallel_web_tools.core import credentials
from parallel_web_tools.core.credentials import (
    CURRENT_VERSION,
    LEGACY_ORG_ID,
    ControlApiTokens,
    Credentials,
    OrgCredentials,
    _migrate_v0,
    delete,
    get_selected_api_key,
    load,
    save,
    set_api_key_for_org,
)


@pytest.fixture
def creds_file(tmp_path, monkeypatch):
    """Patch CREDENTIALS_FILE to a tmp path for isolation."""
    path = tmp_path / "credentials.json"
    monkeypatch.setattr(credentials, "CREDENTIALS_FILE", path)
    return path


class TestMigrationV0:
    def test_migrate_v0_with_access_token(self):
        result = _migrate_v0({"access_token": "abc123"})
        assert result == {
            "version": CURRENT_VERSION,
            "selected_org_id": LEGACY_ORG_ID,
            "orgs": {LEGACY_ORG_ID: {"api_key": "abc123"}},
        }

    def test_migrate_v0_empty(self):
        result = _migrate_v0({})
        assert result["version"] == CURRENT_VERSION
        assert result["selected_org_id"] is None
        assert result["orgs"] == {}


class TestLoad:
    def test_load_nonexistent_returns_none(self, creds_file):
        assert load() is None

    def test_load_corrupted_returns_none(self, creds_file):
        creds_file.parent.mkdir(parents=True, exist_ok=True)
        creds_file.write_text("not valid json {{{")
        assert load() is None

    def test_load_non_dict_returns_none(self, creds_file):
        creds_file.parent.mkdir(parents=True, exist_ok=True)
        creds_file.write_text(json.dumps(["a", "b"]))
        assert load() is None

    def test_load_v0_migrates_and_persists(self, creds_file):
        creds_file.parent.mkdir(parents=True, exist_ok=True)
        creds_file.write_text(json.dumps({"access_token": "tok_v0"}))

        creds = load()

        assert creds is not None
        assert creds.version == CURRENT_VERSION
        assert creds.selected_org_id == LEGACY_ORG_ID
        assert creds.orgs[LEGACY_ORG_ID].api_key == "tok_v0"

        # Migrated file should now be v1 on disk.
        on_disk = json.loads(creds_file.read_text())
        assert on_disk["version"] == CURRENT_VERSION
        assert on_disk["selected_org_id"] == LEGACY_ORG_ID
        assert on_disk["orgs"][LEGACY_ORG_ID]["api_key"] == "tok_v0"

    def test_load_v1_roundtrip(self, creds_file):
        original = Credentials(
            selected_org_id="org_abc",
            client_id="cid_registered",
            orgs={
                "org_abc": OrgCredentials(
                    api_key="sk_test",
                    control_api=ControlApiTokens(
                        access_token="atk",
                        access_token_expires_at=1710000600,
                        access_token_scopes=["keys:write", "balance:read"],
                        refresh_token="rtk",
                        refresh_token_expires_at=1710604800,
                        authorization_expires_at=1717776000,
                    ),
                )
            },
        )
        save(original)

        loaded = load()
        assert loaded == original
        assert loaded is not None and loaded.client_id == "cid_registered"

    def test_migrated_v0_has_no_client_id(self, creds_file):
        # v0 files never carried a client_id — migration must leave it unset
        # so _ensure_client_id knows to register on the next login.
        creds_file.parent.mkdir(parents=True, exist_ok=True)
        creds_file.write_text(json.dumps({"access_token": "tok_v0"}))
        loaded = load()
        assert loaded is not None
        assert loaded.client_id is None


class TestSave:
    def test_save_creates_parent_dir(self, tmp_path, monkeypatch):
        path = tmp_path / "nested" / "subdir" / "credentials.json"
        monkeypatch.setattr(credentials, "CREDENTIALS_FILE", path)
        save(Credentials())
        assert path.exists()

    def test_save_sets_0600_permissions(self, creds_file):
        save(Credentials(selected_org_id="x", orgs={"x": OrgCredentials(api_key="k")}))
        mode = oct(creds_file.stat().st_mode)[-3:]
        assert mode == "600"

    def test_atomic_write_preserves_existing_on_failure(self, creds_file, monkeypatch):
        # Write an initial valid file.
        save(Credentials(selected_org_id="orig", orgs={"orig": OrgCredentials(api_key="original")}))
        original_contents = creds_file.read_text()

        # Make os.replace blow up during the next save.
        def boom(src, dst):
            raise OSError("simulated failure")

        monkeypatch.setattr(os, "replace", boom)

        with pytest.raises(OSError, match="simulated failure"):
            save(Credentials(selected_org_id="new", orgs={"new": OrgCredentials(api_key="new")}))

        # Original file should be untouched.
        assert creds_file.read_text() == original_contents

        # And there should be no leftover temp files in the parent dir.
        leftovers = [p for p in creds_file.parent.iterdir() if p.name.startswith(".credentials.")]
        assert leftovers == []


class TestDelete:
    def test_delete_existing(self, creds_file):
        save(Credentials())
        assert delete() is True
        assert not creds_file.exists()

    def test_delete_nonexistent(self, creds_file):
        assert delete() is False


class TestHelpers:
    def test_get_selected_api_key_none_when_empty(self, creds_file):
        assert get_selected_api_key() is None

    def test_get_selected_api_key_returns_selected(self, creds_file):
        save(
            Credentials(
                selected_org_id="a",
                orgs={
                    "a": OrgCredentials(api_key="key_a"),
                    "b": OrgCredentials(api_key="key_b"),
                },
            )
        )
        assert get_selected_api_key() == "key_a"

    def test_get_selected_api_key_no_selection(self, creds_file):
        save(Credentials(orgs={"a": OrgCredentials(api_key="key_a")}))
        assert get_selected_api_key() is None

    def test_set_api_key_for_org_creates_and_selects(self, creds_file):
        set_api_key_for_org("org_new", "sk_xyz")
        creds = load()
        assert creds is not None
        assert creds.selected_org_id == "org_new"
        assert creds.orgs["org_new"].api_key == "sk_xyz"

    def test_set_api_key_for_org_preserves_selection(self, creds_file):
        set_api_key_for_org("org_a", "sk_a")  # becomes selected
        set_api_key_for_org("org_b", "sk_b")  # should NOT change selection
        creds = load()
        assert creds is not None
        assert creds.selected_org_id == "org_a"
        assert creds.orgs["org_a"].api_key == "sk_a"
        assert creds.orgs["org_b"].api_key == "sk_b"

    def test_set_api_key_for_org_updates_existing(self, creds_file):
        set_api_key_for_org("org_a", "old_key")
        set_api_key_for_org("org_a", "new_key")
        creds = load()
        assert creds is not None
        assert creds.orgs["org_a"].api_key == "new_key"

    def test_set_api_key_for_org_preserves_control_api(self, creds_file):
        # Seed with a control_api block.
        save(
            Credentials(
                selected_org_id="org_a",
                orgs={
                    "org_a": OrgCredentials(
                        api_key="old",
                        control_api=ControlApiTokens(
                            access_token="atk",
                            refresh_token="rtk",
                        ),
                    )
                },
            )
        )
        set_api_key_for_org("org_a", "new")

        creds = load()
        assert creds is not None
        assert creds.orgs["org_a"].api_key == "new"
        assert creds.orgs["org_a"].control_api.access_token == "atk"
        assert creds.orgs["org_a"].control_api.refresh_token == "rtk"
