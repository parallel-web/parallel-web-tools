"""Shared pytest fixtures for the parallel-web-tools test suite."""

import pytest


@pytest.fixture(autouse=True)
def _isolate_cwd(tmp_path, monkeypatch):
    """Run every test in a fresh tmp dir.

    The research CLI now auto-saves results to ./parallel-research/<run_id> by
    default, so any test that exercises `research run` / `research poll`
    without an explicit `-o` would otherwise drop files into the repo root.
    Running every test from a tmp dir keeps the working tree clean and lets us
    drop the per-test `monkeypatch.chdir(tmp_path)` boilerplate.
    """
    monkeypatch.chdir(tmp_path)
