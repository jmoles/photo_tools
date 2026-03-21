"""Unit tests for dedup.py.

Image fixture helpers create real JPEG files with controlled pixel content
and optional EXIF, so tests cover the key invariant:
  pixel_hash(same_pixels_no_exif) == pixel_hash(same_pixels_with_exif)
  sha256_file(same_pixels_no_exif) != sha256_file(same_pixels_with_exif)
"""

from __future__ import annotations

import hashlib
import io
import struct
from pathlib import Path

import pytest
from PIL import Image

from dedup import (
    _candidate_dirs,
    build_pixel_index,
    build_sha_index,
    pixel_hash,
    scan,
    sha256_file,
)


# ---------------------------------------------------------------------------
# Image fixture helpers
# ---------------------------------------------------------------------------

def make_jpeg(path: Path, color: tuple[int, int, int] = (100, 150, 200),
              size: tuple[int, int] = (8, 8)) -> Path:
    """Save a solid-colour JPEG with no EXIF."""
    img = Image.new('RGB', size, color)
    img.save(path, format='JPEG', quality=95)
    return path


def make_jpeg_with_exif(path: Path, color: tuple[int, int, int] = (100, 150, 200),
                        size: tuple[int, int] = (8, 8),
                        date_str: str = '2023:06:15 10:30:00') -> Path:
    """Save a solid-colour JPEG with a DateTimeOriginal EXIF tag injected."""
    # Build a minimal EXIF APP1 segment with DateTimeOriginal (tag 0x9003)
    # Format: EXIF\x00\x00 + TIFF header + IFD
    date_bytes = date_str.encode('ascii') + b'\x00'  # null-terminated, 20 bytes
    ifd_entry  = struct.pack('<HHI', 0x9003, 2, len(date_bytes))  # tag, ASCII, count
    offset     = 8 + 2 + 12 + 4                                   # from TIFF header start
    ifd        = struct.pack('<H', 1) + ifd_entry + struct.pack('<I', offset) + struct.pack('<I', 0)
    tiff_hdr   = b'II' + struct.pack('<H', 42) + struct.pack('<I', 8)  # little-endian TIFF
    exif_data  = b'Exif\x00\x00' + tiff_hdr + ifd + date_bytes

    img = Image.new('RGB', size, color)
    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=95)
    jpeg_bytes = buf.getvalue()

    # Insert APP1 segment after SOI marker (first 2 bytes)
    app1 = b'\xff\xe1' + struct.pack('>H', len(exif_data) + 2) + exif_data
    patched = jpeg_bytes[:2] + app1 + jpeg_bytes[2:]
    path.write_bytes(patched)
    return path


def make_different_jpeg(path: Path) -> Path:
    """Save a JPEG with visually distinct pixel content."""
    return make_jpeg(path, color=(200, 50, 10))


# ---------------------------------------------------------------------------
# sha256_file
# ---------------------------------------------------------------------------

class TestSha256File:
    def test_consistent(self, tmp_path):
        f = make_jpeg(tmp_path / 'a.jpg')
        assert sha256_file(f) == sha256_file(f)

    def test_differs_for_different_files(self, tmp_path):
        a = make_jpeg(tmp_path / 'a.jpg', color=(10, 20, 30))
        b = make_jpeg(tmp_path / 'b.jpg', color=(40, 50, 60))
        assert sha256_file(a) != sha256_file(b)

    def test_differs_with_exif_vs_without(self, tmp_path):
        """Core invariant: same pixels, different EXIF → different SHA-256."""
        no_exif   = make_jpeg(tmp_path / 'no_exif.jpg')
        with_exif = make_jpeg_with_exif(tmp_path / 'with_exif.jpg')
        assert sha256_file(no_exif) != sha256_file(with_exif)


# ---------------------------------------------------------------------------
# pixel_hash
# ---------------------------------------------------------------------------

class TestPixelHash:
    def test_consistent(self, tmp_path):
        f = make_jpeg(tmp_path / 'a.jpg')
        assert pixel_hash(f) == pixel_hash(f)

    def test_same_for_identical_pixels_different_exif(self, tmp_path):
        """Core invariant: same pixels, different EXIF → same pixel hash."""
        no_exif   = make_jpeg(tmp_path / 'no_exif.jpg')
        with_exif = make_jpeg_with_exif(tmp_path / 'with_exif.jpg')
        assert pixel_hash(no_exif) == pixel_hash(with_exif)

    def test_differs_for_different_pixels(self, tmp_path):
        a = make_jpeg(tmp_path / 'a.jpg', color=(10, 20, 30))
        b = make_jpeg(tmp_path / 'b.jpg', color=(40, 50, 60))
        assert pixel_hash(a) != pixel_hash(b)

    def test_returns_none_for_non_image(self, tmp_path):
        f = tmp_path / 'not_an_image.jpg'
        f.write_bytes(b'this is not a jpeg')
        assert pixel_hash(f) is None


# ---------------------------------------------------------------------------
# build_sha_index / build_pixel_index
# ---------------------------------------------------------------------------

class TestBuildIndexes:
    def test_sha_index_maps_hash_to_path(self, tmp_path):
        f = make_jpeg(tmp_path / 'photo.jpg')
        idx = build_sha_index(tmp_path)
        assert sha256_file(f) in idx
        assert idx[sha256_file(f)] == f

    def test_pixel_index_maps_hash_to_path(self, tmp_path):
        f = make_jpeg(tmp_path / 'photo.jpg')
        idx = build_pixel_index(tmp_path)
        assert pixel_hash(f) in idx

    def test_pixel_index_groups_exif_variants(self, tmp_path):
        """Both EXIF and non-EXIF versions should produce the same pixel hash key."""
        no_exif   = make_jpeg(tmp_path / 'no_exif.jpg')
        with_exif = make_jpeg_with_exif(tmp_path / 'with_exif.jpg')
        idx = build_pixel_index(tmp_path)
        # Both map to the same pixel hash — index will hold whichever was seen last
        assert pixel_hash(no_exif) == pixel_hash(with_exif)
        assert pixel_hash(no_exif) in idx

    def test_ignores_non_photo_files(self, tmp_path):
        (tmp_path / 'notes.txt').write_text('hello')
        idx = build_sha_index(tmp_path)
        assert len(idx) == 0


# ---------------------------------------------------------------------------
# _candidate_dirs
# ---------------------------------------------------------------------------

class TestCandidateDirs:
    def test_dated_filename_returns_year_month_first(self, tmp_path):
        src   = tmp_path / '20230615_103022_photo.jpg'
        dirs  = _candidate_dirs(tmp_path, src)
        assert dirs[0] == tmp_path / '2023' / '06'

    def test_undated_filename_returns_library_root(self, tmp_path):
        src  = tmp_path / 'IMG_0042.jpg'
        dirs = _candidate_dirs(tmp_path, src)
        assert dirs[-1] == tmp_path


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------

class TestScan:
    def test_finds_exact_duplicate(self, tmp_path):
        src_dir = tmp_path / 'source'
        lib_dir = tmp_path / 'library'
        src_dir.mkdir(); lib_dir.mkdir()

        orig = make_jpeg(lib_dir / 'photo.jpg')
        dupe = make_jpeg(src_dir / 'photo_copy.jpg')
        # same bytes
        dupe.write_bytes(orig.read_bytes())

        results = scan(src_dir, lib_dir, dry_run=True)
        assert len(results.get('dupe_sha', [])) == 1
        assert len(results.get('unique', [])) == 0

    def test_finds_pixel_duplicate_with_different_exif(self, tmp_path):
        """A file with EXIF in library vs no-EXIF in source → pixel match."""
        src_dir = tmp_path / 'source'
        lib_dir = tmp_path / 'library'
        src_dir.mkdir(); lib_dir.mkdir()

        make_jpeg_with_exif(lib_dir / 'photo_with_exif.jpg')
        make_jpeg(src_dir / 'photo_no_exif.jpg')  # same pixels, no EXIF

        results = scan(src_dir, lib_dir, dry_run=True)
        assert len(results.get('dupe_pixel', [])) == 1
        assert len(results.get('unique', [])) == 0

    def test_unique_file_not_flagged(self, tmp_path):
        src_dir = tmp_path / 'source'
        lib_dir = tmp_path / 'library'
        src_dir.mkdir(); lib_dir.mkdir()

        make_jpeg(src_dir / 'new_photo.jpg', color=(10, 20, 30))
        make_jpeg(lib_dir / 'other_photo.jpg', color=(40, 50, 60))

        results = scan(src_dir, lib_dir, dry_run=True)
        assert len(results.get('unique', [])) == 1
        assert not results.get('dupe_sha') and not results.get('dupe_pixel')

    def test_execute_moves_dupe_to_dupes_dir(self, tmp_path):
        src_dir  = tmp_path / 'source'
        lib_dir  = tmp_path / 'library'
        dupes_dir = tmp_path / 'dupes'
        src_dir.mkdir(); lib_dir.mkdir()

        orig = make_jpeg(lib_dir / 'photo.jpg')
        dupe = make_jpeg(src_dir / 'photo_copy.jpg')
        dupe.write_bytes(orig.read_bytes())

        scan(src_dir, lib_dir, dupes_dir=dupes_dir, dry_run=False)
        assert not dupe.exists()
        assert (dupes_dir / 'photo_copy.jpg').exists()

    def test_dry_run_does_not_move_files(self, tmp_path):
        src_dir = tmp_path / 'source'
        lib_dir = tmp_path / 'library'
        src_dir.mkdir(); lib_dir.mkdir()

        orig = make_jpeg(lib_dir / 'photo.jpg')
        dupe = make_jpeg(src_dir / 'photo_copy.jpg')
        dupe.write_bytes(orig.read_bytes())

        scan(src_dir, lib_dir, dry_run=True)
        assert dupe.exists()
