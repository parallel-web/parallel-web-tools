"""Shared pytest fixtures for the parallel-web-tools test suite."""

import asyncio
import socket
import subprocess
import urllib.request
import webbrowser

import httpx
import pytest


def _blocked_external_io(kind: str):
    def fail(*args, **kwargs):
        raise AssertionError(f"{kind} is disabled in tests; mock the request instead.")

    return fail


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


@pytest.fixture(autouse=True)
def _block_network(monkeypatch):
    """Prevent accidental outbound network calls during tests.

    Tests should mock the specific transport layer they exercise. If something
    reaches the real socket layer, fail fast instead of hanging on live auth or
    API calls.
    """

    fail = _blocked_external_io("Network access")

    monkeypatch.setattr(socket, "create_connection", fail)
    monkeypatch.setattr(socket, "getaddrinfo", fail)
    monkeypatch.setattr(socket, "gethostbyname", fail)
    monkeypatch.setattr(socket, "gethostbyname_ex", fail)
    monkeypatch.setattr(socket, "gethostbyaddr", fail)
    monkeypatch.setattr(socket, "getnameinfo", fail)
    monkeypatch.setattr(socket.socket, "connect", fail)
    monkeypatch.setattr(socket.socket, "connect_ex", fail)
    monkeypatch.setattr(asyncio, "open_connection", fail)
    monkeypatch.setattr(urllib.request, "urlopen", fail)
    monkeypatch.setattr(httpx, "get", fail)
    monkeypatch.setattr(httpx, "post", fail)
    monkeypatch.setattr(httpx, "request", fail)
    monkeypatch.setattr(httpx, "stream", fail)
    monkeypatch.setattr(httpx.Client, "send", fail)
    monkeypatch.setattr(httpx.AsyncClient, "send", fail)


@pytest.fixture(autouse=True)
def _block_subprocess(monkeypatch):
    """Prevent subprocesses from escaping the in-process test harness."""

    fail = _blocked_external_io("Subprocess execution")

    monkeypatch.setattr(subprocess, "Popen", fail)
    monkeypatch.setattr(subprocess, "run", fail)
    monkeypatch.setattr(subprocess, "call", fail)
    monkeypatch.setattr(subprocess, "check_call", fail)
    monkeypatch.setattr(subprocess, "check_output", fail)


@pytest.fixture(autouse=True)
def _block_browser_launch(monkeypatch):
    """Prevent tests from opening a real browser window."""

    monkeypatch.setattr(webbrowser, "open", lambda *args, **kwargs: True)
    monkeypatch.setattr(webbrowser, "open_new", lambda *args, **kwargs: True)
    monkeypatch.setattr(webbrowser, "open_new_tab", lambda *args, **kwargs: True)
