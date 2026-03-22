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


def rename_xmp(xmp_path: Path, new_xmp_path: Path, old_img_name: str, new_img_name: str) -> None:
    """Rename an XMP sidecar, updating any internal references to the image filename."""
    content = xmp_path.read_text()
    content = content.replace(old_img_name, new_img_name)
    new_xmp_path.write_text(content)
    xmp_path.unlink()


def rename_file(path: Path, dt: datetime.datetime, tag: str, dry_run: bool = False) -> Path | None:
    """Rename path to YYYYMMDD_HHMMSS_tag_original.ext (+ paired XMP sidecar).

    Returns the new path, or None in dry-run mode.
    """
    ext = path.suffix.lstrip('.').lower()
    original_stem = path.stem.lower().strip('_')
    xmp_path = find_xmp(path)
    new_stem = f"{dt.strftime('%Y%m%d')}_{dt.strftime('%H%M%S')}_{tag}_{original_stem}"
    new_path = path.with_name(f"{new_stem}.{ext}")
    new_xmp_path = path.with_name(f"{new_stem}.xmp")

    if dry_run:
        print(f"Rename: {path.name} -> {new_path.name}")
        if xmp_path:
            print(f"Rename: {xmp_path.name} -> {new_xmp_path.name}")
        return None

    path.rename(new_path)
    if xmp_path:
        rename_xmp(xmp_path, new_xmp_path, path.name, new_path.name)
    return new_path


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
