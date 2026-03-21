#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""consolidate.py — Bulk photo library consolidation.

Recursively processes a source directory, renames photo/video files by EXIF
date, organises into YYYY/MM structure, deduplicates by SHA-256 content hash,
and moves sidecars (XMP, pp3, Live Photo MOVs) alongside their originals.

Dry-run by default. Pass --execute to apply changes.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import fcntl
import json
import tomllib
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Iterator

# ---------------------------------------------------------------------------
# Format lists
# ---------------------------------------------------------------------------

PHOTO_EXTS: frozenset[str] = frozenset({
    'cr2', 'cr3',         # Canon RAW
    'raf',                # Fujifilm RAW
    'dng',                # Adobe DNG / Ricoh / iPhone ProRAW
    'heic',               # iPhone HEIC
    'jpg', 'jpeg',        # JPEG
    'tif', 'tiff',        # TIFF
    'png',                # PNG
    'webp',               # WebP
    'bmp',                # BMP
})

VIDEO_EXTS: frozenset[str] = frozenset({
    'mov', 'mp4', 'm4v', 'mpg', 'mpeg', 'avi', 'wmv',
})

REVIEW_EXTS: frozenset[str] = frozenset({
    'psd',                # Photoshop
    'lrcat',              # Lightroom catalog
    'afphoto',            # Affinity Photo
    'xcf',                # GIMP
})

SKIP_EXTS: frozenset[str] = frozenset({
    'lrprev', 'lrmprev',  # Lightroom preview cache (regenerable)
    'ds_store',           # macOS metadata
    'ini',                # Windows metadata / camera config
    'db', 'db-journal',   # Thumbs.db / SQLite journals
    'tmp', 'lnk',         # Temp / Windows shortcuts
    'dropbox',            # Dropbox metadata
    'info',               # Misc info files
    'dcp',                # DNG colour profile
    'pto',                # Hugin panorama project
    'txt', 'pdf',         # Documents
    'svg', 'eps',         # Vector
    'gif',                # GIF
})

SKIP_NAMES: frozenset[str] = frozenset({
    '.ds_store', 'thumbs.db', 'desktop.ini', '.dropbox',
})

EXIF_DATE_TAGS = ('DateTimeOriginal', 'CreateDate', 'ModifyDate')

# Matches filenames already renamed by this toolchain: YYYYMMDD_HHMMSS_…
_ALREADY_RENAMED_RE = re.compile(r'^\d{8}_\d{6}_')


def _original_stem(stem: str) -> str:
    """Strip YYYYMMDD_HHMMSS_ prefix from already-renamed files so it isn't doubled."""
    if _ALREADY_RENAMED_RE.match(stem):
        parts = stem.split('_', 2)
        return parts[2] if len(parts) > 2 else stem
    return stem

# Filename date patterns — tried in order, first match wins.
# Named groups: year, month, day; optionally hour, minute, second.
_FILENAME_PATTERNS: list[re.Pattern] = [
    # Full timestamp compact: 20260320_080544
    re.compile(
        r'(?<!\d)(?P<year>(?:19|20)\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])'
        r'[_\-T ](?P<hour>[01]\d|2[0-3])(?P<minute>[0-5]\d)(?P<second>[0-5]\d)(?!\d)'
    ),
    # ISO date + optional time: 2026-03-20 or 2026-03-20_08-05-44
    re.compile(
        r'(?<!\d)(?P<year>(?:19|20)\d{2})[_\-](?P<month>0[1-9]|1[0-2])[_\-](?P<day>0[1-9]|[12]\d|3[01])'
        r'(?:[_\-T ](?P<hour>[01]\d|2[0-3])[_\-:.](?P<minute>[0-5]\d)[_\-:.](?P<second>[0-5]\d))?'
    ),
    # Android / generic prefix: IMG_20260320_080544, VID_…, PANO_…
    re.compile(
        r'(?:IMG|VID|PANO|BURST)[_\-](?P<year>(?:19|20)\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])'
        r'[_\-](?P<hour>[01]\d|2[0-3])(?P<minute>[0-5]\d)(?P<second>[0-5]\d)'
    ),
    # Plain 8-digit date: 20260320
    re.compile(
        r'(?<!\d)(?P<year>(?:19|20)\d{2})(?P<month>0[1-9]|1[0-2])(?P<day>0[1-9]|[12]\d|3[01])(?!\d)'
    ),
]

DEFAULT_CACHE  = 'consolidate_cache.db'
DEFAULT_BATCH  = 500

# ---------------------------------------------------------------------------
# Enums + dataclasses
# ---------------------------------------------------------------------------

class FileCategory(Enum):
    PHOTO  = 'photo'
    VIDEO  = 'video'
    XMP    = 'xmp'
    PP3    = 'pp3'
    THM    = 'thm'
    REVIEW = 'review'
    SKIP   = 'skip'


class DateTier(Enum):
    EXIF     = 1   # Embedded EXIF (DateTimeOriginal / CreateDate / ModifyDate)
    XMP      = 2   # EXIF from XMP sidecar
    FILENAME = 3   # Regex-parsed from filename
    MTIME    = 4   # Filesystem modification time


_TIER_SIGNAL: dict[DateTier, str] = {
    DateTier.EXIF:     '',
    DateTier.XMP:      '',
    DateTier.FILENAME: 'fndate',
    DateTier.MTIME:    'mtime',
}


@dataclass
class DateResult:
    dt:   datetime.datetime
    tier: DateTier


@dataclass
class ProcessContext:
    dest_root:        Path
    source_root:      Path
    tag:              str | None
    cache:            'CacheDB'
    hash_index:       dict[str, str]
    claimed_dests:    set[str]
    claimed_sidecars: set[Path]
    dry_run:          bool


# ---------------------------------------------------------------------------
# SQLite cache
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed (
    source_path  TEXT PRIMARY KEY,
    fingerprint  TEXT NOT NULL,
    dest_path    TEXT NOT NULL,
    content_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS hashes (
    content_hash TEXT PRIMARY KEY,
    dest_path    TEXT NOT NULL
);
"""


class CacheDB:
    def __init__(self, path: Path) -> None:
        self._conn = sqlite3.connect(path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def get_processed(self, source: Path) -> tuple[str, str, str] | None:
        return self._conn.execute(
            'SELECT fingerprint, dest_path, content_hash FROM processed WHERE source_path = ?',
            (str(source),),
        ).fetchone()

    def insert_processed(self, source: Path, fp: str, dest: Path, content_hash: str) -> None:
        self._conn.execute(
            'INSERT OR REPLACE INTO processed '
            '(source_path, fingerprint, dest_path, content_hash) VALUES (?, ?, ?, ?)',
            (str(source), fp, str(dest), content_hash),
        )
        self._conn.commit()

    def get_hash(self, content_hash: str) -> str | None:
        row = self._conn.execute(
            'SELECT dest_path FROM hashes WHERE content_hash = ?',
            (content_hash,),
        ).fetchone()
        return row[0] if row else None

    def insert_hash(self, content_hash: str, dest: Path) -> None:
        self._conn.execute(
            'INSERT OR IGNORE INTO hashes (content_hash, dest_path) VALUES (?, ?)',
            (content_hash, str(dest)),
        )
        self._conn.commit()

    def load_hash_index(self) -> dict[str, str]:
        rows = self._conn.execute('SELECT content_hash, dest_path FROM hashes').fetchall()
        return {r[0]: r[1] for r in rows}

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

def fingerprint(path: Path) -> str:
    st = path.stat()
    return f'{st.st_size}|{st.st_mtime}'


def compute_hash(path: Path, chunk: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        while data := f.read(chunk):
            h.update(data)
    return h.hexdigest()


def walk_source(source_root: Path) -> Iterator[Path]:
    """Walk source recursively, following symlinks, deduplicating by real path."""
    seen_dirs:  set[Path] = set()
    seen_files: set[Path] = set()
    for dirpath_str, dirnames, filenames in os.walk(source_root, followlinks=True):
        dirpath  = Path(dirpath_str)
        real_dir = dirpath.resolve()
        if real_dir in seen_dirs:
            dirnames.clear()   # prevent recursion into already-visited dirs
            continue
        seen_dirs.add(real_dir)
        dirnames.sort()        # deterministic order
        for name in sorted(filenames):
            path      = dirpath / name
            real_path = path.resolve()
            if real_path in seen_files:
                continue
            seen_files.add(real_path)
            yield path


# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------

def classify(path: Path) -> FileCategory:
    if path.name.lower() in SKIP_NAMES:
        return FileCategory.SKIP
    ext = path.suffix.lstrip('.').lower()
    if not ext:
        return FileCategory.SKIP
    if ext in SKIP_EXTS:
        return FileCategory.SKIP
    if ext == 'xmp':
        return FileCategory.XMP
    if ext == 'pp3':
        return FileCategory.PP3
    if ext == 'thm':
        return FileCategory.THM
    if ext in PHOTO_EXTS:
        return FileCategory.PHOTO
    if ext in VIDEO_EXTS:
        return FileCategory.VIDEO
    if ext in REVIEW_EXTS:
        return FileCategory.REVIEW
    return FileCategory.SKIP


# ---------------------------------------------------------------------------
# Sidecar discovery
# ---------------------------------------------------------------------------

def _find_ext(path: Path, *exts: str) -> Path | None:
    for ext in exts:
        c = path.with_suffix('.' + ext)
        if c.is_file():
            return c
    return None


def find_xmp(path: Path) -> Path | None:
    # photo.xmp (standard) or photo.jpg.xmp (Lightroom/darktable double-extension)
    for candidate in [
        path.with_suffix('.xmp'),
        path.with_suffix('.XMP'),
        path.parent / (path.name + '.xmp'),
        path.parent / (path.name + '.XMP'),
    ]:
        if candidate.is_file():
            return candidate
    return None


def find_pp3(path: Path) -> Path | None:
    return _find_ext(path, 'pp3', 'PP3')


def find_live_mov(path: Path) -> Path | None:
    if path.suffix.lower() != '.heic':
        return None
    return _find_ext(path, 'mov', 'MOV')


# ---------------------------------------------------------------------------
# Date extraction — fallback chain
# ---------------------------------------------------------------------------

def _parse_exif_dt(s: str) -> datetime.datetime | None:
    if not s:
        return None
    s = s.strip()
    # Strip timezone suffix: +HH:MM, -HH:MM, or Z
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


def _date_from_exif_dict(data: dict) -> DateResult | None:
    for tag in EXIF_DATE_TAGS:
        val = data.get(tag, '')
        if val:
            dt = _parse_exif_dt(str(val))
            if dt:
                return DateResult(dt=dt, tier=DateTier.EXIF)
    return None


def _date_from_xmp(xmp_path: Path) -> DateResult | None:
    try:
        proc = subprocess.run(
            ['exiftool', '-json', '-q', '-DateTimeOriginal', '-CreateDate', str(xmp_path)],
            capture_output=True, text=True, timeout=30,
        )
        data = json.loads(proc.stdout)
        if data:
            result = _date_from_exif_dict(data[0])
            if result:
                return DateResult(dt=result.dt, tier=DateTier.XMP)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, IndexError, OSError):
        pass
    return None


def _date_from_filename(path: Path) -> DateResult | None:
    name = path.stem
    for pattern in _FILENAME_PATTERNS:
        m = pattern.search(name)
        if not m:
            continue
        g = m.groupdict()
        try:
            dt = datetime.datetime(
                int(g['year']), int(g['month']), int(g['day']),
                int(g.get('hour') or 0),
                int(g.get('minute') or 0),
                int(g.get('second') or 0),
            )
            if 1980 <= dt.year <= 2035:
                return DateResult(dt=dt, tier=DateTier.FILENAME)
        except ValueError:
            continue
    return None


def _date_from_mtime(path: Path) -> DateResult | None:
    try:
        dt = datetime.datetime.fromtimestamp(path.stat().st_mtime)
        return DateResult(dt=dt, tier=DateTier.MTIME)
    except OSError:
        return None


def get_date(path: Path, exif_data: dict, xmp_path: Path | None) -> DateResult | None:
    result = _date_from_exif_dict(exif_data)
    if result:
        return result
    if xmp_path:
        result = _date_from_xmp(xmp_path)
        if result:
            return result
    result = _date_from_filename(path)
    if result:
        return result
    return _date_from_mtime(path)


# ---------------------------------------------------------------------------
# Destination path construction
# ---------------------------------------------------------------------------

def build_filename(
    dt:            datetime.datetime,
    tier:          DateTier,
    tag:           str | None,
    original_stem: str,
    ext:           str,
    hash6:         str | None = None,
) -> str:
    parts: list[str] = [dt.strftime('%Y%m%d'), dt.strftime('%H%M%S')]
    if tag:
        parts.append(tag)
    parts.append(original_stem.lower().strip('_'))
    signal = _TIER_SIGNAL[tier]
    if signal:
        parts.append(signal)
    if hash6:
        parts.append(hash6)
    return '_'.join(parts) + '.' + ext.lower()


def dest_dir_for(
    dt:        datetime.datetime,
    tier:      DateTier,
    root:      Path,
    is_video:  bool = False,
) -> Path:
    if is_video:
        base = root / 'movies'
    else:
        base = root

    if tier == DateTier.MTIME:
        return base / '_undated'
    return base / dt.strftime('%Y') / dt.strftime('%m')


def unique_dest(
    dest_dir:     Path,
    filename:     str,
    content_hash: str,
    claimed:      set[str],
) -> Path:
    dest = dest_dir / filename
    if str(dest) not in claimed and not dest.exists():
        return dest
    stem   = Path(filename).stem
    suffix = Path(filename).suffix
    return dest_dir / f'{stem}_{content_hash[:6]}{suffix}'


# ---------------------------------------------------------------------------
# exiftool batch
# ---------------------------------------------------------------------------

def batch_exiftool(paths: list[Path], batch_size: int = DEFAULT_BATCH) -> dict[Path, dict]:
    results: dict[Path, dict] = {}
    tags = [f'-{t}' for t in EXIF_DATE_TAGS] + ['-FileModifyDate', '-Make', '-Model']
    for i in range(0, len(paths), batch_size):
        batch = paths[i : i + batch_size]
        cmd   = ['exiftool', '-json', '-q'] + tags + [str(p) for p in batch]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            data = json.loads(proc.stdout) if proc.stdout.strip() else []
            for item in data:
                results[Path(item['SourceFile'])] = item
        except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError) as exc:
            logging.error('exiftool batch error: %s', exc)
    return results


# ---------------------------------------------------------------------------
# XMP internal reference update
# ---------------------------------------------------------------------------

def update_xmp_ref(xmp_path: Path, old_name: str, new_name: str) -> None:
    try:
        text = xmp_path.read_text(encoding='utf-8', errors='replace')
        xmp_path.write_text(text.replace(old_name, new_name), encoding='utf-8')
    except OSError as exc:
        logging.warning('Could not update XMP reference in %s: %s', xmp_path, exc)


# ---------------------------------------------------------------------------
# Move helpers
# ---------------------------------------------------------------------------

def _do_move(src: Path, dest: Path, dry_run: bool) -> None:
    if dry_run:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), dest)


def move_to_review(path: Path, ctx: ProcessContext) -> None:
    try:
        rel = path.relative_to(ctx.source_root)
    except ValueError:
        rel = Path(path.name)
    dest = ctx.dest_root / '_review' / rel
    logging.info('REVIEW   %s -> %s', path, dest)
    _do_move(path, dest, ctx.dry_run)


# ---------------------------------------------------------------------------
# Core per-file processing
# ---------------------------------------------------------------------------

def _cache_hit(path: Path, cache: 'CacheDB') -> bool:
    """True if this path has a valid, up-to-date cache entry with dest still on disk."""
    real   = path.resolve()
    cached = cache.get_processed(real)
    if not cached:
        return False
    cached_fp, cached_dest, _ = cached
    try:
        return cached_fp == fingerprint(real) and Path(cached_dest).exists()
    except FileNotFoundError:
        return False


def process_file(path: Path, category: FileCategory, exif_data: dict, ctx: ProcessContext) -> str:
    """Process one photo or video file. Returns action string for counters."""
    log  = logging.getLogger(__name__)
    real = path.resolve()

    if not real.exists():
        log.warning('SKIP     broken symlink: %s', path)
        return 'SKIP'

    # Resume: valid cache hit
    fp     = fingerprint(real)
    cached = ctx.cache.get_processed(real)
    if cached:
        cached_fp, cached_dest, _ = cached
        if cached_fp == fp and Path(cached_dest).exists():
            log.debug('CACHED   %s', path)
            for sc in (find_xmp(path), find_pp3(path), find_live_mov(path)):
                if sc:
                    ctx.claimed_sidecars.add(sc)
            return 'CACHED'

    # Discover sidecars
    xmp_path = find_xmp(path)
    pp3_path = find_pp3(path)
    live_mov = find_live_mov(path) if category == FileCategory.PHOTO else None
    for sc in (xmp_path, pp3_path, live_mov):
        if sc:
            ctx.claimed_sidecars.add(sc)

    # Content hash
    try:
        content_hash = compute_hash(real)
    except OSError as exc:
        log.error('ERROR    hashing %s: %s', path, exc)
        return 'ERROR'

    # Deduplication — verify the recorded original still exists on disk
    existing = ctx.hash_index.get(content_hash) or ctx.cache.get_hash(content_hash)
    if existing and not Path(existing).exists():
        existing = None
    if existing:
        date_result = get_date(path, exif_data, xmp_path)
        if date_result and date_result.tier != DateTier.MTIME:
            dupe_dir = ctx.dest_root / 'dupes' / date_result.dt.strftime('%Y') / date_result.dt.strftime('%m')
        else:
            dupe_dir = ctx.dest_root / 'dupes' / '_undated'
        dt   = date_result.dt   if date_result else datetime.datetime.fromtimestamp(path.stat().st_mtime)
        tier = date_result.tier if date_result else DateTier.MTIME
        fname     = build_filename(dt, tier, ctx.tag, _original_stem(path.stem), path.suffix.lstrip('.'))
        dupe_dest = dupe_dir / fname
        log.info('DUPE     %s -> %s  (original: %s)', path, dupe_dest, existing)
        _do_move(path, dupe_dest, ctx.dry_run)
        for sc, sc_ext in ((xmp_path, '.xmp'), (pp3_path, '.pp3'), (live_mov, '.mov')):
            if sc:
                sc_dest = dupe_dest.with_suffix(sc_ext)
                if sc_ext == '.xmp' and not ctx.dry_run:
                    update_xmp_ref(sc, path.name, dupe_dest.name)
                log.info('  SIDECAR  %s -> %s', sc, sc_dest)
                _do_move(sc, sc_dest, ctx.dry_run)
        return 'DUPE'

    # Date extraction
    date_result = get_date(path, exif_data, xmp_path)
    if date_result is None:
        log.warning('NO_DATE  %s', path)
        return 'NO_DATE'

    # Build destination
    is_video = (category == FileCategory.VIDEO)
    d_dir    = dest_dir_for(date_result.dt, date_result.tier, ctx.dest_root, is_video)
    fname    = build_filename(date_result.dt, date_result.tier, ctx.tag, _original_stem(path.stem), path.suffix.lstrip('.'))
    dest     = unique_dest(d_dir, fname, content_hash, ctx.claimed_dests)

    ctx.claimed_dests.add(str(dest))
    log.info('MOVE     tier=%d %s -> %s', date_result.tier.value, path, dest)

    sidecars: list[tuple[Path, Path, str]] = []
    for sc, sc_ext in ((xmp_path, '.xmp'), (pp3_path, '.pp3'), (live_mov, '.mov')):
        if sc:
            sc_dest = dest.with_suffix(sc_ext)
            sidecars.append((sc, sc_dest, sc_ext))
            log.info('  SIDECAR  %s -> %s', sc, sc_dest)

    if not ctx.dry_run:
        if not path.exists():
            log.warning('SKIP     source vanished before move: %s', path)
            return 'SKIP'
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), dest)
        for sc, sc_dest, sc_ext in sidecars:
            if sc_ext == '.xmp':
                update_xmp_ref(sc, path.name, dest.name)
            sc_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(sc), sc_dest)
        ctx.cache.insert_processed(real, fp, dest, content_hash)
        ctx.cache.insert_hash(content_hash, dest)

    ctx.hash_index[content_hash] = str(dest)
    return 'MOVE'


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _acquire_lock(lock_path: Path) -> bool:
    """Acquire an exclusive non-blocking lock file. Returns True on success."""
    fh = open(lock_path, 'w')
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        fh.close()
        return False


def setup_logging(log_path: str | None) -> None:
    if not log_path:
        ts       = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_path = f'consolidate_{ts}.log'
    fmt = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(
        level=logging.DEBUG,
        format=fmt,
        handlers=[
            logging.FileHandler(log_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info('Log: %s', log_path)


# ---------------------------------------------------------------------------
# Config file loading
# ---------------------------------------------------------------------------

_CONFIG_FILE = Path('config.toml')


def _load_config(path: Path = _CONFIG_FILE) -> dict:
    """Load config.toml if present; return empty dict if not found."""
    if not path.exists():
        return {}
    with path.open('rb') as f:
        return tomllib.load(f)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    cfg = _load_config()
    paths   = cfg.get('paths',   {})
    options = cfg.get('options', {})

    p = argparse.ArgumentParser(
        description='Consolidate and organise a photo library by EXIF date.',
        epilog='Settings in config.toml are used as defaults; CLI args override them.',
    )
    p.add_argument('--source',
                   default=paths.get('source'),
                   help='Source directory')
    p.add_argument('--dest',
                   default=paths.get('dest'),
                   help='Destination root')
    p.add_argument('--tag',
                   default=options.get('tag') or None,
                   help='Optional tag embedded in filenames')
    p.add_argument('--execute',    action='store_true',
                   help='Apply changes (default: dry-run)')
    p.add_argument('--hash-cache',
                   default=paths.get('hash_cache', DEFAULT_CACHE),
                   help='SQLite cache path (default: %(default)s)')
    p.add_argument('--batch-size',
                   default=options.get('batch_size', DEFAULT_BATCH), type=int,
                   help='exiftool batch size (default: %(default)s)')
    p.add_argument('--log',
                   help='Log file path (default: auto-named in cwd)')
    p.add_argument('--config',
                   default=str(_CONFIG_FILE),
                   help='Config file path (default: %(default)s)')
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args    = parse_args()
    dry_run = not args.execute

    setup_logging(args.log)
    log = logging.getLogger(__name__)

    # Prevent concurrent runs from racing each other
    lock_path = Path(args.hash_cache).with_suffix('.lock')
    if not _acquire_lock(lock_path):
        log.error('Another consolidate.py is already running (lock: %s). Exiting.', lock_path)
        sys.exit(1)

    if not args.source or not args.dest:
        log.error(
            'source and dest must be set via config.toml or --source/--dest. '
            'Copy config.example.toml to config.toml and fill in your paths.'
        )
        sys.exit(1)

    source_root = Path(args.source)
    dest_root   = Path(args.dest)
    tag         = args.tag.lower() if args.tag else None

    if dry_run:
        log.info('DRY RUN — no files will be changed. Pass --execute to apply.')

    if not source_root.is_dir():
        log.error('Source directory does not exist: %s', source_root)
        sys.exit(1)

    cache      = CacheDB(Path(args.hash_cache))
    hash_index = cache.load_hash_index()

    # --- Walk and classify ---
    log.info('Walking %s ...', source_root)
    by_cat: dict[FileCategory, list[Path]] = {c: [] for c in FileCategory}
    for path in walk_source(source_root):
        by_cat[classify(path)].append(path)

    photos = by_cat[FileCategory.PHOTO]
    videos = by_cat[FileCategory.VIDEO]

    log.info(
        'Found: %d photos, %d videos, %d XMPs, %d PP3s, %d THMs, %d review, %d skip',
        len(photos), len(videos),
        len(by_cat[FileCategory.XMP]),  len(by_cat[FileCategory.PP3]),
        len(by_cat[FileCategory.THM]),  len(by_cat[FileCategory.REVIEW]),
        len(by_cat[FileCategory.SKIP]),
    )

    # --- Batch EXIF — skip already-cached files to save time on resume ---
    todo = [p for p in photos + videos if p.exists() and not _cache_hit(p, cache)]
    log.info(
        'Running exiftool on %d files (%d already cached)...',
        len(todo), len(photos) + len(videos) - len(todo),
    )
    exif_map = batch_exiftool(todo, batch_size=args.batch_size)

    # Helper to look up exif data tolerating path normalisation differences
    def _exif(p: Path) -> dict:
        return exif_map.get(p) or exif_map.get(p.resolve()) or {}

    # --- Build HEIC stem set for Live Photo detection ---
    heic_stems = {p.stem.lower() for p in photos if p.suffix.lower() == '.heic'}

    claimed_sidecars: set[Path]  = set()
    claimed_dests:    set[str]   = set()
    counters:         dict[str, int] = {}

    def _count(action: str) -> None:
        counters[action] = counters.get(action, 0) + 1

    ctx = ProcessContext(
        dest_root=dest_root, source_root=source_root, tag=tag,
        cache=cache, hash_index=hash_index,
        claimed_dests=claimed_dests, claimed_sidecars=claimed_sidecars,
        dry_run=dry_run,
    )

    # --- Process photos ---
    for i, path in enumerate(photos):
        if i % 1000 == 0 and i > 0:
            log.info('Progress: %d / %d photos', i, len(photos))
        _count(process_file(path, FileCategory.PHOTO, _exif(path), ctx))

    # --- Process videos ---
    for path in videos:
        # Skip Live Photo MOVs — already handled as HEIC sidecars
        if path in claimed_sidecars:
            log.debug('LIVE_MOV %s (claimed by HEIC)', path)
            _count('LIVE_MOV')
            continue
        if path.suffix.lower() == '.mov' and path.stem.lower() in heic_stems:
            claimed_sidecars.add(path)
            log.debug('LIVE_MOV %s (stem match)', path)
            _count('LIVE_MOV')
            continue
        _count(process_file(path, FileCategory.VIDEO, _exif(path), ctx))

    # --- Unclaimed sidecars -> _review ---
    for path in by_cat[FileCategory.XMP] + by_cat[FileCategory.PP3]:
        if path not in claimed_sidecars:
            log.info('ORPHAN   %s -> _review', path)
            move_to_review(path, ctx)
            _count('ORPHAN')

    # --- THM files ---
    all_stems = {p.stem.lower() for p in photos + videos}
    for path in by_cat[FileCategory.THM]:
        if path.stem.lower() in all_stems:
            log.info('DISCARD  paired THM: %s', path)
            if not dry_run:
                path.unlink(missing_ok=True)
            _count('THM_DISCARD')
        else:
            move_to_review(path, ctx)
            _count('THM_REVIEW')

    # --- Review files ---
    for path in by_cat[FileCategory.REVIEW]:
        move_to_review(path, ctx)
        _count('REVIEW')

    # --- Summary ---
    log.info('=== SUMMARY ===')
    for action, count in sorted(counters.items()):
        log.info('  %-15s %d', action, count)

    cache.close()


if __name__ == '__main__':
    main()
