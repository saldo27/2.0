"""Playwright fixtures for Streamlit e2e tests."""

import socket
import subprocess
import time

import pytest


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _wait_for_server(port, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("localhost", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.5)
    return False


@pytest.fixture(scope="session")
def app_port():
    return _find_free_port()


@pytest.fixture(scope="session")
def streamlit_app(app_port):
    """Start the Streamlit app for the test session, tear down when done."""
    proc = subprocess.Popen(
        [
            "uv",
            "run",
            "streamlit",
            "run",
            "src/saldo27/app_streamlit.py",
            f"--server.port={app_port}",
            "--server.headless=true",
            "--browser.gatherUsageStats=false",
            "--global.developmentMode=false",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if not _wait_for_server(app_port):
        proc.terminate()
        pytest.fail(f"Streamlit app did not start on port {app_port}")

    yield f"http://localhost:{app_port}"

    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture
def app_page(page, streamlit_app):
    """Navigate to the running Streamlit app and wait for it to render."""
    page.goto(streamlit_app, wait_until="networkidle")
    # Wait for the Streamlit app frame to be ready
    page.wait_for_selector("[data-testid='stAppViewContainer']", timeout=15000)
    return page
