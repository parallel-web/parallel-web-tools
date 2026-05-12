"""Regression tests for the shared pytest safety harness."""

import asyncio
import socket
import subprocess
import urllib.request
import webbrowser

import httpx
import pytest


class TestNoExternalIoHarness:
    def test_blocks_low_level_socket_connections(self):
        with pytest.raises(AssertionError, match="Network access is disabled"):
            socket.create_connection(("example.com", 443), timeout=1)

        with pytest.raises(AssertionError, match="Network access is disabled"):
            socket.getaddrinfo("example.com", 443)

    def test_blocks_high_level_http_clients(self):
        with pytest.raises(AssertionError, match="Network access is disabled"):
            urllib.request.urlopen("https://example.com")

        with pytest.raises(AssertionError, match="Network access is disabled"):
            httpx.get("https://example.com")

        with pytest.raises(AssertionError, match="Network access is disabled"):
            asyncio.run(asyncio.open_connection("example.com", 443))

    def test_blocks_subprocesses(self):
        with pytest.raises(AssertionError, match="Subprocess execution is disabled"):
            subprocess.run(["true"], check=False)

    def test_browser_launches_are_stubbed(self):
        assert webbrowser.open("https://example.com") is True
        assert webbrowser.open_new("https://example.com") is True
        assert webbrowser.open_new_tab("https://example.com") is True
