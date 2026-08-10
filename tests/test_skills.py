"""Tests for skills helper module."""

import hashlib
import json
from contextlib import contextmanager

import pytest

import parallel_web_tools.core.skills as skills


class TestRepoRef:
    def test_uses_default_ref(self, monkeypatch):
        monkeypatch.delenv(skills.SKILLS_REPO_REF_ENV, raising=False)
        assert skills.get_skills_repo_ref() == skills.DEFAULT_SKILLS_REPO_REF

    def test_uses_env_ref_override(self, monkeypatch):
        monkeypatch.setenv(skills.SKILLS_REPO_REF_ENV, "feature/test-branch")
        assert skills.get_skills_repo_ref() == "feature/test-branch"

    def test_ignores_blank_env_ref(self, monkeypatch):
        monkeypatch.setenv(skills.SKILLS_REPO_REF_ENV, "   ")
        assert skills.get_skills_repo_ref() == skills.DEFAULT_SKILLS_REPO_REF


class TestIndexUrl:
    def test_uses_default_index_url(self, monkeypatch):
        monkeypatch.delenv(skills.SKILLS_INDEX_URL_ENV, raising=False)
        assert skills.get_skills_index_url() == skills.DEFAULT_SKILLS_INDEX_URL

    def test_uses_env_index_url_override(self, monkeypatch):
        monkeypatch.setenv(skills.SKILLS_INDEX_URL_ENV, "https://example.com/index.json")
        assert skills.get_skills_index_url() == "https://example.com/index.json"


class TestResolveInstallDir:
    def test_global_uses_home_agents_skills(self, monkeypatch, tmp_path):
        monkeypatch.delenv(skills.GLOBAL_SKILLS_DIR_ENV, raising=False)
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)
        assert skills.resolve_install_dir(project=False) == tmp_path / ".agents" / "skills"

    def test_global_uses_env_override(self, monkeypatch):
        monkeypatch.setenv(skills.GLOBAL_SKILLS_DIR_ENV, "~/custom-skills")
        expected = skills.Path("~/custom-skills").expanduser()
        assert skills.resolve_install_dir(project=False) == expected

    def test_project_uses_detected_root(self, tmp_path):
        project_root = tmp_path / "repo"
        nested = project_root / "src" / "module"
        nested.mkdir(parents=True)
        (project_root / "pyproject.toml").write_text("[project]\nname='x'\n")

        assert skills.resolve_install_dir(project=True, start=nested) == project_root / ".agents" / "skills"

    def test_project_fails_without_root_markers(self, tmp_path):
        start = tmp_path / "no-root" / "subdir"
        start.mkdir(parents=True)

        with pytest.raises(skills.SkillsInstallLocationError):
            skills.resolve_install_dir(project=True, start=start)


def _make_index() -> dict:
    return {
        "channel": "main",
        "skills": [
            {
                "name": "parallel-web-search",
                "skill_url": "https://skills.parallel.ai/parallel-web-search/SKILL.md",
            },
            {
                "name": "parallel-web-extract",
                "skill_url": "https://skills.parallel.ai/parallel-web-extract/SKILL.md",
            },
        ],
    }


@contextmanager
def _fake_skills_client():
    yield object()


class TestCdnInstall:
    def test_list_remote_skills_from_index(self, monkeypatch):
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())

        assert skills.list_remote_skills("main") == ["parallel-web-extract", "parallel-web-search"]

    def test_list_remote_skills_ignores_ref_override(self, monkeypatch):
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())

        assert skills.list_remote_skills("feature/test-branch") == ["parallel-web-extract", "parallel-web-search"]

    def test_install_skills_from_index(self, monkeypatch, tmp_path):
        install_dir = tmp_path / "install"

        def fake_download_skill_file(client, skill_name: str, skill_url: str) -> bytes:
            assert skill_name == "parallel-web-search"
            assert skill_url.endswith("/parallel-web-search/SKILL.md")
            return b"search"

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_file", fake_download_skill_file)

        result = skills.install_skills(install_dir, selected_skills=["parallel-web-search"], ref="main")

        assert result["ref"] == "main"
        assert result["installed_skills"] == ["parallel-web-search"]
        assert (install_dir / "parallel-web-search" / "SKILL.md").read_text() == "search"
        assert not (install_dir / "parallel-web-extract").exists()

    def test_install_subset_removes_previously_managed_skills(self, monkeypatch, tmp_path):
        install_dir = tmp_path / "install"

        def fake_download_skill_file(client, skill_name: str, skill_url: str) -> bytes:
            return skill_name.encode()

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_file", fake_download_skill_file)

        skills.install_skills(install_dir, ref="main")
        skills.install_skills(install_dir, selected_skills=["parallel-web-search"], ref="main")

        assert (install_dir / "parallel-web-search").exists()
        assert not (install_dir / "parallel-web-extract").exists()

        result = skills.uninstall_skills(install_dir)

        assert result["removed_skills"] == ["parallel-web-search"]
        assert not any(path.name.startswith("parallel-web-") for path in install_dir.iterdir())

    def test_install_skills_rejects_unknown_names(self, monkeypatch, tmp_path):
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())

        with pytest.raises(skills.SkillsInputError, match="Unknown skills requested"):
            skills.install_skills(tmp_path / "install", selected_skills=["does-not-exist"], ref="main")

    def test_install_skills_ignores_ref_override(self, monkeypatch, tmp_path):
        install_dir = tmp_path / "install"

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_file", lambda client, skill_name, skill_url: b"search")

        result = skills.install_skills(install_dir, selected_skills=["parallel-web-search"], ref="feature/test-branch")

        assert result["ref"] == "main"


class TestRemoteChannel:
    def test_get_remote_skills_channel(self, monkeypatch):
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())

        assert skills.get_remote_skills_channel() == "main"


class TestUninstall:
    def test_uninstall_only_removes_manifest_managed_skills(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        managed = install_dir / "parallel-web-search"
        unmanaged = install_dir / "custom-skill"
        managed.mkdir(parents=True)
        unmanaged.mkdir(parents=True)
        (managed / "SKILL.md").write_text("managed")
        (unmanaged / "SKILL.md").write_text("custom")

        manifest = {
            "installed_skills": ["parallel-web-search"],
            "ref": "main",
        }
        (install_dir / skills.MANIFEST_FILE_NAME).write_text(json.dumps(manifest))

        result = skills.uninstall_skills(install_dir)

        assert result["count"] == 1
        assert result["removed_skills"] == ["parallel-web-search"]
        assert not managed.exists()
        assert unmanaged.exists()
        assert not (install_dir / skills.MANIFEST_FILE_NAME).exists()


def _make_manifest_index() -> dict:
    return {
        "channel": "main",
        "skills": [
            {
                "name": "migrate-to-parallel",
                "skill_url": "https://skills.parallel.ai/migrate-to-parallel/SKILL.md",
                "manifest_url": "https://skills.parallel.ai/migrate-to-parallel/manifest.json",
            },
        ],
    }


def _make_manifest(files: list[dict]) -> dict:
    return {"schema_version": 1, "name": "migrate-to-parallel", "files": files}


def _file_entry(path: str, content: bytes) -> dict:
    return {
        "path": path,
        "url": f"https://skills.parallel.ai/migrate-to-parallel/{path}",
        "sha256": hashlib.sha256(content).hexdigest(),
        "size": len(content),
    }


class TestManifestInstall:
    def _patch(self, monkeypatch, manifest: dict, contents: dict[str, bytes]) -> None:
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_manifest_index())
        monkeypatch.setattr(skills, "_fetch_json", lambda client, url, description: manifest)
        monkeypatch.setattr(
            skills,
            "_download_skill_file",
            lambda client, skill_name, url: contents[url.rsplit("/migrate-to-parallel/", 1)[1]],
        )

    def test_install_downloads_every_manifest_file(self, monkeypatch, tmp_path):
        install_dir = tmp_path / "install"
        contents = {
            "SKILL.md": b"skill body",
            "references/exa.md": b"exa reference",
            "scripts/scan_provider_usage.py": b"print('scan')",
            "agents/openai.yaml": b"name: openai",
        }
        manifest = _make_manifest([_file_entry(path, body) for path, body in contents.items()])
        self._patch(monkeypatch, manifest, contents)

        result = skills.install_skills(install_dir)

        skill_dir = install_dir / "migrate-to-parallel"
        assert result["file_count"] == 4
        assert (skill_dir / "SKILL.md").read_bytes() == b"skill body"
        assert (skill_dir / "references" / "exa.md").read_bytes() == b"exa reference"
        assert (skill_dir / "scripts" / "scan_provider_usage.py").read_bytes() == b"print('scan')"
        assert (skill_dir / "agents" / "openai.yaml").read_bytes() == b"name: openai"

    def test_install_rejects_checksum_mismatch(self, monkeypatch, tmp_path):
        contents = {"SKILL.md": b"skill body"}
        manifest = _make_manifest([_file_entry("SKILL.md", b"different bytes")])
        self._patch(monkeypatch, manifest, contents)

        with pytest.raises(skills.SkillsDownloadError, match="Checksum mismatch"):
            skills.install_skills(tmp_path / "install")

    def test_install_rejects_path_traversal(self, monkeypatch, tmp_path):
        contents = {"SKILL.md": b"skill body"}
        manifest = _make_manifest(
            [
                _file_entry("SKILL.md", b"skill body"),
                {"path": "../../escaped.md", "url": "https://skills.parallel.ai/x/escaped.md", "sha256": ""},
            ]
        )
        self._patch(monkeypatch, manifest, contents)

        with pytest.raises(skills.SkillsDownloadError, match="unsafe file path"):
            skills.install_skills(tmp_path / "install")

    def test_install_rejects_manifest_without_skill_md(self, monkeypatch, tmp_path):
        contents = {"references/exa.md": b"exa reference"}
        manifest = _make_manifest([_file_entry("references/exa.md", b"exa reference")])
        self._patch(monkeypatch, manifest, contents)

        with pytest.raises(skills.SkillsDownloadError, match="does not include a SKILL.md"):
            skills.install_skills(tmp_path / "install")

    def test_install_falls_back_when_manifest_has_no_files(self, monkeypatch, tmp_path):
        install_dir = tmp_path / "install"
        contents = {"SKILL.md": b"skill body"}
        self._patch(monkeypatch, _make_manifest([]), contents)

        result = skills.install_skills(install_dir)

        assert result["file_count"] == 1
        assert (install_dir / "migrate-to-parallel" / "SKILL.md").read_bytes() == b"skill body"
