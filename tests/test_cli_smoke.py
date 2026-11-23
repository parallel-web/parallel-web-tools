"""Safety and cost gating for the manual live smoke runner (no network calls)."""

import importlib
import json
import subprocess
from pathlib import Path

import pytest


@pytest.fixture
def live(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[1] / "scripts"))
    return importlib.import_module("cli_smoke_live")


def test_cost_gate(live):
    costly = live.LIVE_SPECS["findall"]
    assert not live.eligible(costly, set())
    assert not live.eligible(costly, {"README.md"})
    assert live.eligible(costly, {"parallel_web_tools/core/findall.py"})
    assert live.eligible(costly, set(), forced=True)
    assert live.eligible(live.LIVE_SPECS["research"], set())


def test_changed_sources_includes_worktree_and_untracked(live, monkeypatch, tmp_path):
    calls = []

    def git(args, **kwargs):
        calls.append(args)
        output = {"merge-base": "abc", "diff": "parallel_web_tools/core/findall.py\n", "ls-files": "new.py\n"}
        return subprocess.CompletedProcess(args, 0, stdout=output[args[1]])

    monkeypatch.setattr(live.subprocess, "run", git)
    assert live.changed_sources(tmp_path) == {"parallel_web_tools/core/findall.py", "new.py"}
    assert calls[1] == ("git", "diff", "--name-only", "abc", "--")


def test_changed_sources_fails_closed(live, monkeypatch, tmp_path):
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(1, "git")

    monkeypatch.setattr(live.subprocess, "run", fail)
    with pytest.raises(SystemExit, match="Cannot determine live cost gate"):
        live.changed_sources(tmp_path)


@pytest.mark.parametrize("rows", [[], [{"id": "memory_1", "content": "IANA"}]])
def test_memory_normal_and_empty(live, monkeypatch, tmp_path, rows):
    def invoke(cli, args, env, cwd, **kwargs):
        assert env["HOME"] == env["USERPROFILE"] == str(tmp_path)
        assert Path(env["CLAUDE_CONFIG_DIR"]).is_relative_to(tmp_path)
        assert Path(env["PARALLEL_SKILLS_GLOBAL_DIR"]).is_relative_to(tmp_path)
        assert Path(env["XDG_CONFIG_HOME"]).is_relative_to(tmp_path)
        return json.dumps({"results": rows})

    monkeypatch.setattr(live, "invoke_cli", invoke)
    live.run_live(Path("parallel-cli"), {}, tmp_path, ["memory"])


def test_memory_invalid_shape(live, monkeypatch, tmp_path):
    monkeypatch.setattr(live, "invoke_cli", lambda *a, **k: '{"results": null}')
    with pytest.raises(SystemExit, match="unexpected live response shape"):
        live.run_live(Path("parallel-cli"), {}, tmp_path, ["memory"])


def test_unavailable_endpoint_is_a_skip(live, monkeypatch, tmp_path, capsys):
    def unavailable(*args, **kwargs):
        raise SystemExit("API error: 403 Forbidden")

    monkeypatch.setattr(live, "invoke_cli", unavailable)
    live.run_live(Path("parallel-cli"), {}, tmp_path, ["memory"])
    output = capsys.readouterr().out
    assert "SKIP remaining memory commands: endpoint unavailable" in output
    assert "memory checks finished" not in output


def test_empty_monitors_do_not_get_or_fetch_events(live, monkeypatch, tmp_path, capsys):
    def invoke(cli, args, *a, **kw):
        assert args[:2] == ("monitor", "list")
        return '{"monitors": []}'

    monkeypatch.setattr(live, "invoke_cli", invoke)
    live.run_live(Path("parallel-cli"), {}, tmp_path, ["monitor"])
    assert "SKIP monitor get/events: no monitors" in capsys.readouterr().out


def test_unknown_case(live, tmp_path):
    with pytest.raises(SystemExit, match="Unknown live cases"):
        live.run_live(Path("parallel-cli"), {}, tmp_path, ["balance add"])
