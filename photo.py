"""photo.py — Shared utilities for shoot.py and organize.py."""

from __future__ import annotations

import datetime
import re
from pathlib import Path

# Matches filenames already renamed by this toolchain: YYYYMMDD_HHMMSS_…
ALREADY_RENAMED_RE = re.compile(r'^\d{8}_\d{6}_')

# Photo extensions recognised by both scripts (shoot.py subset)
SHOOT_PHOTO_EXTS: frozenset[str] = frozenset({
    'cr2', 'cr3',   # Canon RAW
    'nef',          # Nikon RAW
    'arw',          # Sony RAW
    'raf',          # Fujifilm RAW
    'orf',          # Olympus RAW
    'rw2',          # Panasonic RAW
    'dng',          # Adobe DNG
    'jpg', 'jpeg',  # JPEG
})


def parse_exif_dt(s: str) -> datetime.datetime | None:
    """Parse an EXIF or ISO 8601 datetime string, returning None on failure or epoch."""
    if not s:
        return None
    s = s.strip()
    s = re.sub(r'[+-]\d{2}:\d{2}$', '', s)
    s = re.sub(r'Z$', '', s)
    for fmt in (
        '%Y:%m:%d %H:%M:%S',    # standard EXIF
        '%Y-%m-%dT%H:%M:%S',    # ISO 8601 with T separator
        '%Y-%m-%d %H:%M:%S',    # ISO 8601 with space separator
    ):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt if dt.year > 1970 else None
        except ValueError:
            continue
    return None


def find_xmp(path: Path) -> Path | None:
    """Return the XMP sidecar for path, or None.

    Checks both standard (photo.xmp) and Lightroom/darktable
    double-extension style (photo.jpg.xmp).
    """
    for candidate in [
        path.with_suffix('.xmp'),
        path.with_suffix('.XMP'),
        path.parent / (path.name + '.xmp'),
        path.parent / (path.name + '.XMP'),
    ]:
        if candidate.is_file():
            return candidate
    return None
