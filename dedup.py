#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = ["Pillow>=10.0"]
# ///
"""dedup.py — Deep duplicate scan using pixel-hash comparison.

Scans a source directory for photos that are pixel-identical to files
already in a library directory, even when EXIF metadata differs between
copies. Uses two-stage detection:

  Stage 1: SHA-256 of full file bytes  (fast — catches exact copies)
  Stage 2: SHA-256 of decoded pixels   (thorough — catches EXIF-stripped copies)

Dry-run by default. Pass --execute to move duplicates to --dupes-dir.

Usage:
  uv run dedup.py --source /path/to/scan --library /path/to/library
  uv run dedup.py --source /path/to/scan --library /path/to/library --execute
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import shutil
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

from PIL import Image, UnidentifiedImageError

PHOTO_EXTS: frozenset[str] = frozenset({
    'jpg', 'jpeg', 'png', 'bmp', 'tif', 'tiff',
    'heic', 'raf', 'cr2', 'cr3', 'dng', 'webp',
})


# ---------------------------------------------------------------------------
# Hashing
# ---------------------------------------------------------------------------

def sha256_file(path: Path, chunk: int = 4 * 1024 * 1024) -> str:
    """SHA-256 of raw file bytes."""
    h = hashlib.sha256()
    with path.open('rb') as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()


def pixel_hash(path: Path) -> str | None:
    """SHA-256 of decoded RGB pixel data — ignores all metadata."""
    try:
        with Image.open(path) as img:
            return hashlib.sha256(img.convert('RGB').tobytes()).hexdigest()
    except (UnidentifiedImageError, Exception):
        return None


# ---------------------------------------------------------------------------
# Index building
# ---------------------------------------------------------------------------

def build_sha_index(directory: Path) -> dict[str, Path]:
    """Map SHA-256 → path for all photos under directory."""
    idx: dict[str, Path] = {}
    for f in directory.rglob('*'):
        if f.is_file() and f.suffix.lower().lstrip('.') in PHOTO_EXTS:
            idx[sha256_file(f)] = f
    return idx


def build_pixel_index(directory: Path) -> dict[str, Path]:
    """Map pixel-hash → path for all photos under directory."""
    idx: dict[str, Path] = {}
    for f in directory.rglob('*'):
        if f.is_file() and f.suffix.lower().lstrip('.') in PHOTO_EXTS:
            ph = pixel_hash(f)
            if ph:
                idx[ph] = f
    return idx


def load_cache_sha_index(cache_path: Path) -> dict[str, Path]:
    """Load SHA-256 → dest_path from an organize.py SQLite cache."""
    if not cache_path.exists():
        return {}
    conn = sqlite3.connect(cache_path)
    rows = conn.execute('SELECT content_hash, dest_path FROM hashes').fetchall()
    conn.close()
    return {r[0]: Path(r[1]) for r in rows}


# ---------------------------------------------------------------------------
# Scan
# ---------------------------------------------------------------------------

def scan(
    source: Path,
    library: Path,
    cache_path: Path | None = None,
    dupes_dir: Path | None = None,
    dry_run: bool = True,
) -> dict[str, list]:
    """Scan source for duplicates of files in library.

    Returns dict with keys: 'dupe_sha', 'dupe_pixel', 'unique', 'error'.
    Each value is a list of (source_path, matched_library_path | None).
    """
    log = logging.getLogger(__name__)

    if dupes_dir is None:
        dupes_dir = library / 'dupes'

    # Stage 1 index: cache (fast) + full library SHA scan
    log.info('Loading SHA index …')
    sha_index: dict[str, Path] = {}
    if cache_path:
        sha_index.update(load_cache_sha_index(cache_path))
    sha_index.update(build_sha_index(library))

    # Stage 2 index: pixel hashes — built lazily per subdirectory
    pixel_indexes: dict[Path, dict[str, Path]] = {}

    def _pixel_idx(directory: Path) -> dict[str, Path]:
        if directory not in pixel_indexes:
            log.info('Building pixel index for %s …', directory)
            pixel_indexes[directory] = build_pixel_index(directory)
        return pixel_indexes[directory]

    results: dict[str, list] = defaultdict(list)

    source_files = sorted([
        f for f in source.rglob('*')
        if f.is_file() and f.suffix.lower().lstrip('.') in PHOTO_EXTS
    ])

    for i, src in enumerate(source_files):
        if i % 50 == 0 and i > 0:
            log.info('Progress: %d / %d', i, len(source_files))

        # Stage 1: full-file SHA-256
        h = sha256_file(src)
        lib_match = sha_index.get(h)
        if lib_match and lib_match.resolve() != src.resolve():
            log.info('DUPE_SHA   %s  →  %s', src.name, lib_match)
            results['dupe_sha'].append((src, lib_match))
            if not dry_run:
                _move_to_dupes(src, dupes_dir)
            continue

        # Stage 2: pixel hash against candidate library subdirectories
        ph = pixel_hash(src)
        if ph is None:
            log.warning('ERROR      could not decode %s', src.name)
            results['error'].append((src, None))
            continue

        found_pixel = False
        for lib_subdir in _candidate_dirs(library, src):
            if not lib_subdir.exists():
                continue
            pidx = _pixel_idx(lib_subdir)
            pix_match = pidx.get(ph)
            if pix_match and pix_match.resolve() != src.resolve():
                log.info('DUPE_PIX   %s  →  %s', src.name, pix_match)
                results['dupe_pixel'].append((src, pix_match))
                if not dry_run:
                    _move_to_dupes(src, dupes_dir)
                found_pixel = True
                break

        if not found_pixel:
            log.info('UNIQUE     %s', src.name)
            results['unique'].append((src, None))

    return dict(results)


def _candidate_dirs(library: Path, src: Path) -> list[Path]:
    """Return library subdirs most likely to contain a duplicate of src."""
    # If source name starts with YYYYMM, check that year/month first
    name = src.name
    if len(name) >= 6 and name[:4].isdigit() and name[4:6].isdigit():
        yr, mo = name[:4], name[4:6]
        return [library / yr / mo, library / yr, library]
    return [library]


def _move_to_dupes(src: Path, dupes_dir: Path) -> None:
    dest = dupes_dir / src.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Avoid collision
    if dest.exists():
        dest = dest.with_stem(dest.stem + '_dup')
    shutil.move(str(src), dest)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--source',  required=True, type=Path,
                   help='Directory to scan for duplicates')
    p.add_argument('--library', required=True, type=Path,
                   help='Library to compare against')
    p.add_argument('--cache',   type=Path, default=None,
                   help='organize.py SQLite cache for fast SHA lookup')
    p.add_argument('--dupes-dir', type=Path, default=None,
                   help='Where to move duplicates (default: library/dupes/)')
    p.add_argument('--execute', action='store_true',
                   help='Move duplicates (default: dry-run)')
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dry_run = not args.execute

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)-8s %(message)s',
        handlers=[logging.StreamHandler()],
    )
    log = logging.getLogger(__name__)

    if dry_run:
        log.info('DRY RUN — no files will be moved. Pass --execute to apply.')

    if not args.source.is_dir():
        log.error('Source does not exist: %s', args.source)
        sys.exit(1)
    if not args.library.is_dir():
        log.error('Library does not exist: %s', args.library)
        sys.exit(1)

    results = scan(
        source=args.source,
        library=args.library,
        cache_path=args.cache,
        dupes_dir=args.dupes_dir,
        dry_run=dry_run,
    )

    log.info('=== SUMMARY ===')
    log.info('  DUPE_SHA     %d', len(results.get('dupe_sha', [])))
    log.info('  DUPE_PIXEL   %d', len(results.get('dupe_pixel', [])))
    log.info('  UNIQUE       %d', len(results.get('unique', [])))
    log.info('  ERROR        %d', len(results.get('error', [])))


if __name__ == '__main__':
    main()
