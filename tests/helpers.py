"""Shared constants and helper functions for photo-tools tests."""

from __future__ import annotations

import subprocess
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths to real sample files
# ---------------------------------------------------------------------------

SAMPLES_DIR   = Path(__file__).parent.parent / 'samples'
FUJIFILM_RAF  = SAMPLES_DIR / '_DSF4989.RAF'
RICOH_DNG     = SAMPLES_DIR / 'R0000573.DNG'
IPHONE_HEIC   = SAMPLES_DIR / 'IMG_0879.HEIC'
IPHONE_PRORAW = SAMPLES_DIR / 'IMG_0878.DNG'
CANON_CR2     = SAMPLES_DIR / 'IMG_1103.CR2'

# ---------------------------------------------------------------------------
# Minimal 1×1 JPEG bytes (no EXIF) — used as a base for synthetic fixtures
# ---------------------------------------------------------------------------

# fmt: off
MINIMAL_JPEG: bytes = bytes([
    0xFF,0xD8,0xFF,0xE0,0x00,0x10,0x4A,0x46,0x49,0x46,0x00,0x01,
    0x01,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0xFF,0xDB,0x00,0x43,
    0x00,0x08,0x06,0x06,0x07,0x06,0x05,0x08,0x07,0x07,0x07,0x09,
    0x09,0x08,0x0A,0x0C,0x14,0x0D,0x0C,0x0B,0x0B,0x0C,0x19,0x12,
    0x13,0x0F,0x14,0x1D,0x1A,0x1F,0x1E,0x1D,0x1A,0x1C,0x1C,0x20,
    0x24,0x2E,0x27,0x20,0x22,0x2C,0x23,0x1C,0x1C,0x28,0x37,0x29,
    0x2C,0x30,0x31,0x34,0x34,0x34,0x1F,0x27,0x39,0x3D,0x38,0x32,
    0x3C,0x2E,0x33,0x34,0x32,0xFF,0xC0,0x00,0x0B,0x08,0x00,0x01,
    0x00,0x01,0x01,0x01,0x11,0x00,0xFF,0xC4,0x00,0x1F,0x00,0x00,
    0x01,0x05,0x01,0x01,0x01,0x01,0x01,0x01,0x00,0x00,0x00,0x00,
    0x00,0x00,0x00,0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,0x08,
    0x09,0x0A,0x0B,0xFF,0xDA,0x00,0x08,0x01,0x01,0x00,0x00,0x3F,
    0x00,0xFB,0xD8,0xFF,0xD9,
])
# fmt: on

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_jpeg_with_exif(path: Path, date_str: str = '2023:06:15 10:30:00') -> Path:
    """Write a minimal JPEG then inject date tags via exiftool.

    Writes both DateTimeOriginal and ModifyDate (→ 'Image DateTime' in exifread)
    to match what real cameras produce.
    """
    path.write_bytes(MINIMAL_JPEG)
    subprocess.run(
        [
            'exiftool', '-overwrite_original',
            f'-DateTimeOriginal={date_str}',
            f'-ModifyDate={date_str}',
            str(path),
        ],
        check=True, capture_output=True,
    )
    return path


def make_jpeg_no_exif(path: Path) -> Path:
    """Write a minimal JPEG with no EXIF metadata."""
    path.write_bytes(MINIMAL_JPEG)
    subprocess.run(
        ['exiftool', '-overwrite_original', '-all=', str(path)],
        check=True, capture_output=True,
    )
    return path


def make_jpeg_with_gps(
    path: Path,
    date_str: str = '2026:03:10 09:14:00',
    lat: float = 34.6937,   # Osaka, Japan
    lon: float = 135.5023,
) -> Path:
    """Write a minimal JPEG with DateTimeOriginal and GPS coordinates."""
    path.write_bytes(MINIMAL_JPEG)
    subprocess.run(
        [
            'exiftool', '-overwrite_original',
            f'-DateTimeOriginal={date_str}',
            f'-GPSLatitude={abs(lat)}',
            f'-GPSLatitudeRef={"N" if lat >= 0 else "S"}',
            f'-GPSLongitude={abs(lon)}',
            f'-GPSLongitudeRef={"E" if lon >= 0 else "W"}',
            str(path),
        ],
        check=True, capture_output=True,
    )
    return path


def inject_exif_date(path: Path, date_str: str) -> Path:
    """Inject DateTimeOriginal and ModifyDate into an existing file via exiftool."""
    subprocess.run(
        [
            'exiftool', '-overwrite_original',
            f'-DateTimeOriginal={date_str}',
            f'-ModifyDate={date_str}',
            str(path),
        ],
        check=True, capture_output=True,
    )
    return path


def make_xmp_sidecar(image_path: Path, date_str: str = '2023:06:15 10:30:00') -> Path:
    """Create a minimal XMP sidecar with a known DateTimeOriginal."""
    xmp_path = image_path.with_suffix('.xmp')
    subprocess.run(
        ['exiftool', '-o', str(xmp_path), f'-DateTimeOriginal={date_str}', str(image_path)],
        check=True, capture_output=True,
    )
    return xmp_path


