"""pytest fixtures for photo-tools tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from helpers import (  # noqa: F401  (re-exported for test files that need them)
    CANON_CR2,
    FUJIFILM_RAF,
    IPHONE_HEIC,
    IPHONE_PRORAW,
    RICOH_DNG,
    make_jpeg_no_exif,
    make_jpeg_with_exif,
    make_xmp_sidecar,
)


@pytest.fixture()
def src(tmp_path: Path) -> Path:
    """Empty source directory."""
    d = tmp_path / 'source'
    d.mkdir()
    return d


@pytest.fixture()
def dst(tmp_path: Path) -> Path:
    """Empty destination directory."""
    d = tmp_path / 'dest'
    d.mkdir()
    return d
