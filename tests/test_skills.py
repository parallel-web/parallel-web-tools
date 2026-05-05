"""Tests for skills helper module."""

import json

import pytest

from parallel_web_tools.core import skills


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
    def test_uses_gh_token_when_present(self, monkeypatch):
        monkeypatch.setenv(skills.GITHUB_TOKEN_ENV, "ghp_test123")
        headers = skills._github_headers()
        assert headers["Authorization"] == "Bearer ghp_test123"

    def test_omits_auth_header_when_token_missing(self, monkeypatch):
        monkeypatch.delenv(skills.GITHUB_TOKEN_ENV, raising=False)
        headers = skills._github_headers()
        assert "Authorization" not in headers


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
