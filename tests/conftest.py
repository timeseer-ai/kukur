"""Shared pytest fixtures."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import PurePath, PureWindowsPath

import pytest


@pytest.fixture
def windows_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make PurePath behave as it does on Windows.

    pathlib selects the flavour of PurePath using os.name at instantiation time.
    """
    monkeypatch.setattr(os, "name", "nt")
    assert isinstance(PurePath("a"), PureWindowsPath)
