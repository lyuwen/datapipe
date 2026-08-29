"""Shared pytest fixtures."""

from __future__ import annotations

import os
import sys

# Ensure the package is importable when running tests without installation.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402


@pytest.fixture
def tmp_workspace(tmp_path):
    """A temp directory guaranteed to persist for the duration of a test."""
    return tmp_path


def pytest_configure(config):
    """Quiet progress bars by default in tests (set --progress to override)."""
    pass
