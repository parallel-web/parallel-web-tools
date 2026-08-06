"""Tests for skills helper module."""

import json
import shutil
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


def _make_skill(install_dir, name: str):
    skill_dir = install_dir / name
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(f"# {name}\n")
    return skill_dir


class TestLinkIntoClaudeCode:
    def test_noop_when_claude_dir_absent(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["claude_dir_present"] is False
        assert result["linked"] == []
        assert not (tmp_path / ".claude").exists()

    def test_symlinks_skills_into_claude_dir(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        skill_dir = _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        link_path = tmp_path / ".claude" / "skills" / "parallel-web-search"
        assert result["linked"] == ["parallel-web-search"]
        assert result["warnings"] == []
        assert link_path.is_symlink()
        assert link_path.resolve() == skill_dir.resolve()
        assert (link_path / "SKILL.md").read_text() == "# parallel-web-search\n"

    def test_falls_back_to_home_without_project_root(self, monkeypatch, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"])

        assert result["claude_skills_dir"] == str(tmp_path / ".claude" / "skills")
        assert result["linked"] == ["parallel-web-search"]

    def test_is_idempotent(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()

        skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)
        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["linked"] == []
        assert result["skipped"] == ["parallel-web-search"]
        assert result["warnings"] == []

    def test_refreshes_symlink_pointing_elsewhere(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        skill_dir = _make_skill(install_dir, "parallel-web-search")
        stale_target = _make_skill(tmp_path / "elsewhere", "parallel-web-search")
        claude_skills = tmp_path / ".claude" / "skills"
        claude_skills.mkdir(parents=True)
        (claude_skills / "parallel-web-search").symlink_to(stale_target, target_is_directory=True)

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["linked"] == ["parallel-web-search"]
        assert (claude_skills / "parallel-web-search").resolve() == skill_dir.resolve()

    def test_does_not_clobber_existing_real_directory(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        claude_skills = tmp_path / ".claude" / "skills"
        existing = claude_skills / "parallel-web-search"
        existing.mkdir(parents=True)
        (existing / "SKILL.md").write_text("hand-written")

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["linked"] == []
        assert result["skipped"] == ["parallel-web-search"]
        assert len(result["warnings"]) == 1
        assert (existing / "SKILL.md").read_text() == "hand-written"

    def test_skips_when_skills_dir_already_points_at_install_dir(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".claude" / "skills").symlink_to(install_dir, target_is_directory=True)

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["linked"] == []
        assert result["skipped"] == ["parallel-web-search"]
        assert result["warnings"] == []

    def test_ignores_skill_dirs_that_do_not_exist(self, tmp_path):
        (tmp_path / ".claude").mkdir()

        result = skills.link_into_claude_code(tmp_path / ".agents" / "skills", ["ghost"], project_root=tmp_path)

        assert result["linked"] == []
        assert result["skipped"] == []
        assert not (tmp_path / ".claude" / "skills").exists()

    def test_copies_when_symlinks_unavailable(self, monkeypatch, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()

        def unsupported(*args, **kwargs):
            raise OSError("symlink privilege not held")

        monkeypatch.setattr("parallel_web_tools.core.skills.os.symlink", unsupported)

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        link_path = tmp_path / ".claude" / "skills" / "parallel-web-search"
        assert result["copied"] == ["parallel-web-search"]
        assert result["linked"] == []
        assert not link_path.is_symlink()
        assert (link_path / "SKILL.md").read_text() == "# parallel-web-search\n"

    def test_warns_when_copy_fallback_also_fails(self, monkeypatch, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()

        def unsupported(*args, **kwargs):
            raise OSError("nope")

        monkeypatch.setattr("parallel_web_tools.core.skills.os.symlink", unsupported)
        monkeypatch.setattr("parallel_web_tools.core.skills.shutil.copytree", unsupported)

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["linked"] == []
        assert result["copied"] == []
        assert "parallel-web-search" in result["warnings"][0]

    def test_prunes_links_for_skills_no_longer_installed(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        dropped = _make_skill(install_dir, "parallel-web-extract")
        (tmp_path / ".claude").mkdir()
        skills.link_into_claude_code(
            install_dir, ["parallel-web-search", "parallel-web-extract"], project_root=tmp_path
        )

        # Mirror a narrowed --skill install: the dropped skill is removed first.
        shutil.rmtree(dropped)
        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        claude_skills = tmp_path / ".claude" / "skills"
        assert result["pruned"] == ["parallel-web-extract"]
        assert not (claude_skills / "parallel-web-extract").is_symlink()
        assert (claude_skills / "parallel-web-search").is_symlink()

    def test_prune_leaves_foreign_dangling_links_alone(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        claude_skills = tmp_path / ".claude" / "skills"
        claude_skills.mkdir(parents=True)
        foreign = claude_skills / "someone-elses-skill"
        foreign.symlink_to(tmp_path / "elsewhere" / "gone", target_is_directory=True)

        result = skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["pruned"] == []
        assert foreign.is_symlink()


class TestUnlinkFromClaudeCode:
    def test_removes_links_it_created(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        skill_dir = _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()
        skills.link_into_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        # Mirror uninstall order: the canonical skill is removed first.
        shutil.rmtree(skill_dir)
        result = skills.unlink_from_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["unlinked"] == ["parallel-web-search"]
        assert not (tmp_path / ".claude" / "skills" / "parallel-web-search").exists()
        assert not (tmp_path / ".claude" / "skills" / "parallel-web-search").is_symlink()

    def test_leaves_unrelated_skills_alone(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        claude_skills = tmp_path / ".claude" / "skills"
        own_skill = claude_skills / "parallel-web-search"
        own_skill.mkdir(parents=True)
        (own_skill / "SKILL.md").write_text("hand-written")
        foreign_target = _make_skill(tmp_path / "elsewhere", "parallel-web-extract")
        (claude_skills / "parallel-web-extract").symlink_to(foreign_target, target_is_directory=True)

        result = skills.unlink_from_claude_code(
            install_dir, ["parallel-web-search", "parallel-web-extract"], project_root=tmp_path
        )

        assert result["unlinked"] == []
        assert sorted(result["skipped"]) == ["parallel-web-extract", "parallel-web-search"]
        assert (own_skill / "SKILL.md").read_text() == "hand-written"
        assert (claude_skills / "parallel-web-extract").is_symlink()

    def test_noop_when_claude_skills_dir_absent(self, tmp_path):
        result = skills.unlink_from_claude_code(
            tmp_path / ".agents" / "skills", ["parallel-web-search"], project_root=tmp_path
        )

        assert result == {"claude_skills_dir": None, "unlinked": [], "skipped": [], "warnings": []}

    def test_noop_when_skills_dir_points_at_install_dir(self, tmp_path):
        install_dir = tmp_path / ".agents" / "skills"
        _make_skill(install_dir, "parallel-web-search")
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".claude" / "skills").symlink_to(install_dir, target_is_directory=True)

        result = skills.unlink_from_claude_code(install_dir, ["parallel-web-search"], project_root=tmp_path)

        assert result["unlinked"] == []
        assert (install_dir / "parallel-web-search").exists()


class TestClaudeCodeSkillsDir:
    def test_uses_home_by_default(self, monkeypatch, tmp_path):
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)
        assert skills.get_claude_code_skills_dir() == tmp_path / ".claude" / "skills"

    def test_uses_project_root_when_given(self, tmp_path):
        assert skills.get_claude_code_skills_dir(tmp_path / "repo") == tmp_path / "repo" / ".claude" / "skills"


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

        def fake_download_skill_markdown(client, skill_name: str, skill_url: str) -> bytes:
            assert skill_name == "parallel-web-search"
            assert skill_url.endswith("/parallel-web-search/SKILL.md")
            return b"search"

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_markdown", fake_download_skill_markdown)

        result = skills.install_skills(install_dir, selected_skills=["parallel-web-search"], ref="main")

        assert result["ref"] == "main"
        assert result["installed_skills"] == ["parallel-web-search"]
        assert (install_dir / "parallel-web-search" / "SKILL.md").read_text() == "search"
        assert not (install_dir / "parallel-web-extract").exists()

    def test_install_subset_removes_previously_managed_skills(self, monkeypatch, tmp_path):
        install_dir = tmp_path / "install"

        def fake_download_skill_markdown(client, skill_name: str, skill_url: str) -> bytes:
            return skill_name.encode()

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_markdown", fake_download_skill_markdown)

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
        monkeypatch.setattr(skills, "_download_skill_markdown", lambda client, skill_name, skill_url: b"search")

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
