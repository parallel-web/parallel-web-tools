"""Tests for skills helper module."""

import io
import json
import zipfile
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


class TestGithubHeaders:
    def test_uses_expected_github_headers(self, monkeypatch):
        monkeypatch.delenv(skills.GITHUB_TOKEN_ENV, raising=False)
        headers = skills._github_headers()
        assert headers["Accept"] == "application/vnd.github+json"
        assert headers["X-GitHub-Api-Version"] == "2022-11-28"
        assert "Authorization" not in headers

    def test_uses_gh_token_when_present(self, monkeypatch):
        monkeypatch.setenv(skills.GITHUB_TOKEN_ENV, "ghp_test123")
        headers = skills._github_headers()
        assert headers["Authorization"] == "Bearer ghp_test123"


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


def _make_repo_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("parallel-web-parallel-agent-skills-abc123/skills/parallel-web-search/SKILL.md", "search")
        zf.writestr("parallel-web-parallel-agent-skills-abc123/skills/parallel-web-extract/SKILL.md", "extract")
    return buffer.getvalue()


class TestArchiveInstall:
    def test_extract_repo_archive_returns_repo_root(self, tmp_path):
        repo_root = skills._extract_repo_archive(_make_repo_zip(), tmp_path)
        assert repo_root.name == "parallel-web-parallel-agent-skills-abc123"
        assert (repo_root / "skills" / "parallel-web-search" / "SKILL.md").read_text() == "search"

    def test_list_remote_skills_from_archive(self, monkeypatch, tmp_path):
        repo_root = skills._extract_repo_archive(_make_repo_zip(), tmp_path)

        @contextmanager
        def fake_downloaded_repo_root(ref: str):
            assert ref == "feature/test-branch"
            yield repo_root

        monkeypatch.setattr(skills, "_downloaded_repo_root", fake_downloaded_repo_root)
        assert skills.list_remote_skills("feature/test-branch") == ["parallel-web-extract", "parallel-web-search"]

    def test_install_skills_from_archive(self, monkeypatch, tmp_path):
        repo_root = skills._extract_repo_archive(_make_repo_zip(), tmp_path / "archive")
        install_dir = tmp_path / "install"

        @contextmanager
        def fake_downloaded_repo_root(ref: str):
            assert ref == "main"
            yield repo_root

        monkeypatch.setattr(skills, "_downloaded_repo_root", fake_downloaded_repo_root)

        result = skills.install_skills(install_dir, selected_skills=["parallel-web-search"], ref="main")

        assert result["installed_skills"] == ["parallel-web-search"]
        assert (install_dir / "parallel-web-search" / "SKILL.md").read_text() == "search"
        assert not (install_dir / "parallel-web-extract").exists()

    def test_install_subset_removes_previously_managed_skills(self, monkeypatch, tmp_path):
        repo_root = skills._extract_repo_archive(_make_repo_zip(), tmp_path / "archive")
        install_dir = tmp_path / "install"

        @contextmanager
        def fake_downloaded_repo_root(ref: str):
            yield repo_root

        monkeypatch.setattr(skills, "_downloaded_repo_root", fake_downloaded_repo_root)

        skills.install_skills(install_dir, ref="main")
        skills.install_skills(install_dir, selected_skills=["parallel-web-search"], ref="main")

        assert (install_dir / "parallel-web-search").exists()
        assert not (install_dir / "parallel-web-extract").exists()

        result = skills.uninstall_skills(install_dir)

        assert result["removed_skills"] == ["parallel-web-search"]
        assert not any(path.name.startswith("parallel-web-") for path in install_dir.iterdir())

    def test_install_skills_rejects_unknown_names(self, monkeypatch, tmp_path):
        repo_root = skills._extract_repo_archive(_make_repo_zip(), tmp_path / "archive")

        @contextmanager
        def fake_downloaded_repo_root(ref: str):
            yield repo_root

        monkeypatch.setattr(skills, "_downloaded_repo_root", fake_downloaded_repo_root)

        with pytest.raises(skills.SkillsInputError, match="Unknown skills requested"):
            skills.install_skills(tmp_path / "install", selected_skills=["does-not-exist"], ref="main")


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
