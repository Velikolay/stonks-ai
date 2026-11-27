"""Settings every test shares."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def no_frame_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse without the on-disk table cache, so tests never read or write it."""
    monkeypatch.setenv("FILINGS_FRAME_CACHE", "0")
