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


class TestResolveInstallDirs:
    def test_global_adds_claude_dir_when_present(self, monkeypatch, tmp_path):
        monkeypatch.delenv(skills.GLOBAL_SKILLS_DIR_ENV, raising=False)
        monkeypatch.delenv(skills.CLAUDE_CONFIG_DIR_ENV, raising=False)
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)
        (tmp_path / ".claude").mkdir()

        assert skills.resolve_install_dirs(project=False) == [
            tmp_path / ".agents" / "skills",
            tmp_path / ".claude" / "skills",
        ]

    def test_global_skips_claude_dir_when_absent(self, monkeypatch, tmp_path):
        monkeypatch.delenv(skills.GLOBAL_SKILLS_DIR_ENV, raising=False)
        monkeypatch.delenv(skills.CLAUDE_CONFIG_DIR_ENV, raising=False)
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)

        assert skills.resolve_install_dirs(project=False) == [tmp_path / ".agents" / "skills"]

    def test_global_honors_claude_config_dir_env(self, monkeypatch, tmp_path):
        monkeypatch.delenv(skills.GLOBAL_SKILLS_DIR_ENV, raising=False)
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)
        custom_claude = tmp_path / "elsewhere" / "claude-config"
        custom_claude.mkdir(parents=True)
        monkeypatch.setenv(skills.CLAUDE_CONFIG_DIR_ENV, str(custom_claude))

        assert skills.resolve_install_dirs(project=False) == [
            tmp_path / ".agents" / "skills",
            custom_claude / "skills",
        ]

    def test_global_env_override_targets_single_dir(self, monkeypatch, tmp_path):
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)
        (tmp_path / ".claude").mkdir()
        monkeypatch.setenv(skills.GLOBAL_SKILLS_DIR_ENV, str(tmp_path / "custom-skills"))

        assert skills.resolve_install_dirs(project=False) == [tmp_path / "custom-skills"]

    def test_project_adds_claude_dir_when_present(self, tmp_path):
        project_root = tmp_path / "repo"
        nested = project_root / "src" / "module"
        nested.mkdir(parents=True)
        (project_root / "pyproject.toml").write_text("[project]\nname='x'\n")
        (project_root / ".claude").mkdir()

        assert skills.resolve_install_dirs(project=True, start=nested) == [
            project_root / ".agents" / "skills",
            project_root / ".claude" / "skills",
        ]

    def test_project_ignores_claude_config_dir_env(self, monkeypatch, tmp_path):
        project_root = tmp_path / "repo"
        project_root.mkdir()
        (project_root / "pyproject.toml").write_text("[project]\nname='x'\n")
        global_claude = tmp_path / "home" / ".claude"
        global_claude.mkdir(parents=True)
        monkeypatch.setenv(skills.CLAUDE_CONFIG_DIR_ENV, str(global_claude))

        assert skills.resolve_install_dirs(project=True, start=project_root) == [project_root / ".agents" / "skills"]

    def test_symlinked_claude_dir_is_deduped(self, monkeypatch, tmp_path):
        monkeypatch.delenv(skills.GLOBAL_SKILLS_DIR_ENV, raising=False)
        monkeypatch.delenv(skills.CLAUDE_CONFIG_DIR_ENV, raising=False)
        monkeypatch.setattr("parallel_web_tools.core.skills.Path.home", lambda: tmp_path)
        agents_skills = tmp_path / ".agents" / "skills"
        agents_skills.mkdir(parents=True)
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".claude" / "skills").symlink_to(agents_skills, target_is_directory=True)

        assert skills.resolve_install_dirs(project=False) == [agents_skills]


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


class TestMultiTargetInstall:
    def _patch(self, monkeypatch) -> None:
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_file", lambda client, skill_name, url: skill_name.encode())

    def test_install_writes_every_target(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        agents_dir = tmp_path / ".agents" / "skills"
        claude_dir = tmp_path / ".claude" / "skills"

        result = skills.install_skills([agents_dir, claude_dir], selected_skills=["parallel-web-search"])

        assert result["install_dirs"] == [str(agents_dir), str(claude_dir)]
        assert result["file_count"] == 2
        for install_dir in (agents_dir, claude_dir):
            assert (install_dir / "parallel-web-search" / "SKILL.md").read_bytes() == b"parallel-web-search"
            assert (install_dir / skills.MANIFEST_FILE_NAME).exists()

    def test_install_downloads_each_file_once(self, monkeypatch, tmp_path):
        downloaded: list[str] = []

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(
            skills,
            "_download_skill_file",
            lambda client, skill_name, url: (downloaded.append(url), skill_name.encode())[1],
        )

        skills.install_skills([tmp_path / "a", tmp_path / "b", tmp_path / "c"], selected_skills=["parallel-web-search"])

        assert len(downloaded) == 1

    def test_failed_download_leaves_no_target_written(self, monkeypatch, tmp_path):
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())

        def explode(client, skill_name, url):
            raise skills.SkillsDownloadError("network down")

        monkeypatch.setattr(skills, "_download_skill_file", explode)
        agents_dir = tmp_path / ".agents" / "skills"

        with pytest.raises(skills.SkillsDownloadError):
            skills.install_skills([agents_dir, tmp_path / ".claude" / "skills"])

        assert not agents_dir.exists()

    def test_uninstall_clears_every_target(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        agents_dir = tmp_path / ".agents" / "skills"
        claude_dir = tmp_path / ".claude" / "skills"
        skills.install_skills([agents_dir, claude_dir], selected_skills=["parallel-web-search"])

        result = skills.uninstall_skills([agents_dir, claude_dir])

        assert result["removed_skills"] == ["parallel-web-search"]
        assert result["count"] == 1
        for install_dir in (agents_dir, claude_dir):
            assert not (install_dir / "parallel-web-search").exists()
            assert not (install_dir / skills.MANIFEST_FILE_NAME).exists()

    def test_install_prunes_dropped_skills_from_every_target(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        targets = [tmp_path / ".agents" / "skills", tmp_path / ".claude" / "skills"]
        skills.install_skills(targets)
        skills.install_skills(targets, selected_skills=["parallel-web-search"])

        for install_dir in targets:
            assert (install_dir / "parallel-web-search").exists()
            assert not (install_dir / "parallel-web-extract").exists()

    def test_duplicate_targets_are_written_once(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        install_dir = tmp_path / ".agents" / "skills"

        result = skills.install_skills([install_dir, install_dir], selected_skills=["parallel-web-search"])

        assert result["install_dirs"] == [str(install_dir)]
        assert result["file_count"] == 1

    def test_empty_target_list_is_rejected(self, tmp_path):
        with pytest.raises(skills.SkillsInstallLocationError):
            skills.install_skills([])


class TestCollisionGuard:
    def _patch(self, monkeypatch) -> None:
        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_file", lambda client, skill_name, url: skill_name.encode())

    def test_install_leaves_unmanaged_same_named_skill_untouched(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        install_dir = tmp_path / ".claude" / "skills"
        mine = install_dir / "parallel-web-search"
        mine.mkdir(parents=True)
        (mine / "SKILL.md").write_text("hand written")

        result = skills.install_skills([install_dir], selected_skills=["parallel-web-search"])

        assert (mine / "SKILL.md").read_text() == "hand written"
        assert result["installed_skills"] == []
        assert result["count"] == 0
        assert result["file_count"] == 0
        assert result["skipped_skills"] == [{"skill": "parallel-web-search", "install_dir": str(install_dir)}]

    def test_skipped_skill_is_absent_from_manifest_and_survives_uninstall(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        install_dir = tmp_path / ".claude" / "skills"
        mine = install_dir / "parallel-web-search"
        mine.mkdir(parents=True)
        (mine / "SKILL.md").write_text("hand written")

        skills.install_skills([install_dir])
        manifest = json.loads((install_dir / skills.MANIFEST_FILE_NAME).read_text())
        assert "parallel-web-search" not in manifest["installed_skills"]
        assert "parallel-web-extract" in manifest["installed_skills"]

        result = skills.uninstall_skills([install_dir])

        assert (mine / "SKILL.md").read_text() == "hand written"
        assert result["removed_skills"] == ["parallel-web-extract"]

    def test_our_own_skill_is_still_replaced_on_reinstall(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        install_dir = tmp_path / ".agents" / "skills"
        skills.install_skills([install_dir], selected_skills=["parallel-web-search"])
        (install_dir / "parallel-web-search" / "stale.md").write_text("stale")

        result = skills.install_skills([install_dir], selected_skills=["parallel-web-search"])

        assert result["installed_skills"] == ["parallel-web-search"]
        assert result["skipped_skills"] == []
        assert not (install_dir / "parallel-web-search" / "stale.md").exists()

    def test_skip_is_per_directory(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        agents_dir = tmp_path / ".agents" / "skills"
        claude_dir = tmp_path / ".claude" / "skills"
        mine = claude_dir / "parallel-web-search"
        mine.mkdir(parents=True)
        (mine / "SKILL.md").write_text("hand written")

        result = skills.install_skills([agents_dir, claude_dir], selected_skills=["parallel-web-search"])

        assert (agents_dir / "parallel-web-search" / "SKILL.md").read_bytes() == b"parallel-web-search"
        assert (mine / "SKILL.md").read_text() == "hand written"
        assert result["installed_skills"] == ["parallel-web-search"]
        assert result["skipped_skills"] == [{"skill": "parallel-web-search", "install_dir": str(claude_dir)}]

    def test_reinstall_accepts_a_one_shot_iterable(self, monkeypatch, tmp_path):
        self._patch(monkeypatch)
        install_dir = tmp_path / ".agents" / "skills"
        skills.install_skills([install_dir], selected_skills=["parallel-web-search"])

        result = skills.reinstall_skills((path for path in [install_dir]), selected_skills=["parallel-web-search"])

        assert result["removed_skills"] == ["parallel-web-search"]
        assert result["installed_skills"] == ["parallel-web-search"]
        assert (install_dir / "parallel-web-search" / "SKILL.md").exists()


class TestInterruptedInstall:
    def _patch(self, monkeypatch, doomed: str = "") -> None:
        """Patch the CDN, and make writing the doomed skill fail like a full disk would."""
        real = skills._contained_target

        def flaky(skill_name, skill_dir, relative_path):
            if skill_name == doomed:
                raise RuntimeError("disk full")
            return real(skill_name, skill_dir, relative_path)

        monkeypatch.setattr(skills, "_skills_client", _fake_skills_client)
        monkeypatch.setattr(skills, "_fetch_skills_index", lambda client: _make_index())
        monkeypatch.setattr(skills, "_download_skill_file", lambda client, name, url: name.encode())
        monkeypatch.setattr(skills, "_contained_target", flaky)

    def test_crashed_install_is_repaired_rather_than_skipped(self, monkeypatch, tmp_path):
        install_dir = tmp_path / ".claude" / "skills"
        self._patch(monkeypatch, doomed="parallel-web-search")
        with pytest.raises(RuntimeError, match="disk full"):
            skills.install_skills([install_dir])

        monkeypatch.undo()
        self._patch(monkeypatch)
        result = skills.install_skills([install_dir])

        assert result["skipped_skills"] == []
        assert (install_dir / "parallel-web-search" / "SKILL.md").read_bytes() == b"parallel-web-search"

    def test_crashed_install_does_not_claim_a_users_skill(self, monkeypatch, tmp_path):
        install_dir = tmp_path / ".claude" / "skills"
        mine = install_dir / "parallel-web-extract"
        mine.mkdir(parents=True)
        (mine / "SKILL.md").write_text("hand written")
        self._patch(monkeypatch, doomed="parallel-web-search")

        with pytest.raises(RuntimeError, match="disk full"):
            skills.install_skills([install_dir])

        manifest = json.loads((install_dir / skills.MANIFEST_FILE_NAME).read_text())
        assert manifest["installed_skills"] == ["parallel-web-search"]
        assert (mine / "SKILL.md").read_text() == "hand written"


class TestUnsafeManifestPaths:
    @pytest.mark.parametrize(
        "raw_path",
        ["C:/outside/payload", "C:\\outside\\payload", "/etc/passwd", "../escape.md", "a/../../escape.md"],
    )
    def test_rejects_paths_that_escape_the_skill_directory(self, raw_path):
        with pytest.raises(skills.SkillsDownloadError, match="unsafe file path"):
            skills._safe_skill_file_path("migrate-to-parallel", raw_path)

    @pytest.mark.parametrize("raw_path", ["SKILL.md", "references/exa.md", "scripts/scan.py"])
    def test_accepts_ordinary_relative_paths(self, raw_path):
        assert skills._safe_skill_file_path("migrate-to-parallel", raw_path) == raw_path

    def test_contained_target_rejects_escapes_that_slip_past_validation(self, tmp_path):
        skill_dir = tmp_path / "skills" / "migrate-to-parallel"
        skill_dir.mkdir(parents=True)

        with pytest.raises(skills.SkillsDownloadError, match="resolved outside its directory"):
            skills._contained_target("migrate-to-parallel", skill_dir, "../../escaped.md")

    def test_contained_target_allows_nested_paths(self, tmp_path):
        skill_dir = tmp_path / "skills" / "migrate-to-parallel"
        skill_dir.mkdir(parents=True)

        assert skills._contained_target("migrate-to-parallel", skill_dir, "references/exa.md") == (
            skill_dir / "references" / "exa.md"
        )


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
