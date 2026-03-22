#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""ingest.py — Import photos from an SD card to a local staging directory.

Auto-detects the mounted SD card, clusters unimported photos into shoots by
time gap, prompts for a tag per shoot, and copies files (+ sidecars) to
~/Pictures/incoming/<tag>/.

Tracks imports in ~/.config/photo-tools/imports.db to skip files already
imported and to permanently ignore old shoots that are already on the NAS.

Dry-run by default. Pass --execute to copy files.
"""

from __future__ import annotations

import argparse
import datetime
import json
import plistlib
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from photo import ALREADY_RENAMED_RE, parse_exif_dt, rename_file

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VOLUMES_DIR      = Path('/Volumes')
DEFAULT_INCOMING = Path.home() / 'Pictures' / 'incoming'
CONFIG_DIR       = Path.home() / '.config' / 'photo-tools'
DEFAULT_DB_PATH  = CONFIG_DIR / 'imports.db'
GAP_HOURS_DEFAULT = 4.0

PHOTO_EXTS: frozenset[str] = frozenset({
    'cr2', 'cr3', 'raf', 'dng', 'heic',
    'jpg', 'jpeg', 'tif', 'tiff', 'png', 'webp', 'bmp',
})
VIDEO_EXTS: frozenset[str] = frozenset({
    'mov', 'mp4', 'm4v', 'mpg', 'mpeg', 'avi', 'wmv',
})
PRIMARY_EXTS: frozenset[str] = PHOTO_EXTS | VIDEO_EXTS
SIDECAR_EXTS:  frozenset[str] = frozenset({'xmp', 'pp3'})

EXIF_DATE_TAGS = ('DateTimeOriginal', 'CreateDate', 'ModifyDate')

# ---------------------------------------------------------------------------
# SQLite tracking
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS imported_files (
    card_uuid   TEXT    NOT NULL,
    filename    TEXT    NOT NULL,
    size        INTEGER NOT NULL,
    mtime       REAL    NOT NULL,
    tag         TEXT    NOT NULL,
    dest_path   TEXT    NOT NULL,
    imported_at TEXT    NOT NULL,
    PRIMARY KEY (card_uuid, filename, size, mtime)
);
CREATE TABLE IF NOT EXISTS ignored_files (
    card_uuid   TEXT    NOT NULL,
    filename    TEXT    NOT NULL,
    size        INTEGER NOT NULL,
    mtime       REAL    NOT NULL,
    PRIMARY KEY (card_uuid, filename, size, mtime)
);
"""


class IngestDB:
    def __init__(self, path: Path = DEFAULT_DB_PATH) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path)
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def _stat(self, path: Path) -> tuple[int, float]:
        st = path.stat()
        return st.st_size, st.st_mtime

    def is_imported(self, card_uuid: str, path: Path) -> bool:
        size, mtime = self._stat(path)
        return self._conn.execute(
            'SELECT 1 FROM imported_files '
            'WHERE card_uuid=? AND filename=? AND size=? AND mtime=?',
            (card_uuid, path.name, size, mtime),
        ).fetchone() is not None

    def is_ignored(self, card_uuid: str, path: Path) -> bool:
        size, mtime = self._stat(path)
        return self._conn.execute(
            'SELECT 1 FROM ignored_files '
            'WHERE card_uuid=? AND filename=? AND size=? AND mtime=?',
            (card_uuid, path.name, size, mtime),
        ).fetchone() is not None

    def mark_imported(self, card_uuid: str, path: Path, tag: str, dest: Path) -> None:
        size, mtime = self._stat(path)
        self._conn.execute(
            'INSERT OR REPLACE INTO imported_files '
            '(card_uuid, filename, size, mtime, tag, dest_path, imported_at) '
            'VALUES (?,?,?,?,?,?,?)',
            (card_uuid, path.name, size, mtime, tag, str(dest),
             datetime.datetime.now().isoformat()),
        )
        self._conn.commit()

    def mark_ignored(self, card_uuid: str, path: Path) -> None:
        size, mtime = self._stat(path)
        self._conn.execute(
            'INSERT OR REPLACE INTO ignored_files '
            '(card_uuid, filename, size, mtime) VALUES (?,?,?,?)',
            (card_uuid, path.name, size, mtime),
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


# ---------------------------------------------------------------------------
# Card detection
# ---------------------------------------------------------------------------

def find_cards(volumes_dir: Path = VOLUMES_DIR) -> list[Path]:
    """Return mounted volumes that contain a DCIM folder."""
    cards: list[Path] = []
    try:
        for entry in sorted(volumes_dir.iterdir()):
            if entry.is_dir() and (entry / 'DCIM').is_dir():
                cards.append(entry)
    except PermissionError:
        pass
    return cards


def get_card_uuid(card_path: Path) -> str:
    """Return the volume UUID via diskutil, falling back to the volume name."""
    try:
        proc = subprocess.run(
            ['diskutil', 'info', '-plist', str(card_path)],
            capture_output=True, timeout=10,
        )
        info = plistlib.loads(proc.stdout)
        uuid = info.get('VolumeUUID') or info.get('DiskUUID')
        if uuid:
            return str(uuid)
    except Exception:
        pass
    return card_path.name


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def walk_dcim(card_path: Path) -> tuple[list[Path], dict[Path, Path]]:
    """Walk DCIM/ and return (primary_files, sidecar_to_primary_map).

    Sidecars are matched to their primary file by stem.  Both standard
    (photo.xmp) and double-extension (photo.jpg.xmp) styles are handled.
    """
    dcim = card_path / 'DCIM'
    primaries: list[Path] = []
    sidecars:  list[Path] = []

    for path in sorted(dcim.rglob('*')):
        if not path.is_file():
            continue
        ext = path.suffix.lstrip('.').lower()
        if ext in PRIMARY_EXTS:
            primaries.append(path)
        elif ext in SIDECAR_EXTS:
            sidecars.append(path)

    # stem → primary (last write wins on duplicate stems across subdirs)
    stem_map: dict[str, Path] = {p.stem.lower(): p for p in primaries}

    sidecar_map: dict[Path, Path] = {}
    for sc in sidecars:
        stem = sc.stem.lower()
        if stem in stem_map:
            sidecar_map[sc] = stem_map[stem]
        else:
            # double-extension style: photo.jpg.xmp → stem is "photo.jpg"
            parent_stem = Path(stem).stem
            if parent_stem in stem_map:
                sidecar_map[sc] = stem_map[parent_stem]

    return primaries, sidecar_map


# ---------------------------------------------------------------------------
# EXIF reading
# ---------------------------------------------------------------------------

def read_exif(paths: list[Path]) -> dict[Path, dict]:
    """Batch-read date and GPS/geolocation tags from files via exiftool."""
    if not paths:
        return {}

    tags = (
        [f'-{t}' for t in EXIF_DATE_TAGS]
        + ['-GPSLatitude', '-GPSLongitude', '-GeolocationCity', '-GeolocationCountry']
    )
    # -api geolocation enables exiftool's built-in reverse-geocoding (12.74+).
    # Older versions emit a warning to stderr but still produce valid JSON.
    cmd = ['exiftool', '-json', '-q', '-api', 'geolocation'] + tags + [str(p) for p in paths]
    results: dict[Path, dict] = {}
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        for item in json.loads(proc.stdout or '[]'):
            results[Path(item['SourceFile'])] = item
    except FileNotFoundError:
        print('Error: exiftool not found. Install with: brew install exiftool', file=sys.stderr)
    except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
        pass
    return results


def _best_date(path: Path, exif: dict) -> datetime.datetime | None:
    """Return best datetime for a file: EXIF tags first, then mtime."""
    for tag in EXIF_DATE_TAGS:
        dt = parse_exif_dt(str(exif.get(tag, '')))
        if dt:
            return dt
    try:
        return datetime.datetime.fromtimestamp(path.stat().st_mtime)
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Clustering
# ---------------------------------------------------------------------------

def cluster_by_gap(
    dated_files: list[tuple[Path, datetime.datetime]],
    gap_hours: float = GAP_HOURS_DEFAULT,
) -> list[list[Path]]:
    """Group (path, datetime) pairs into clusters separated by gap_hours.

    Files are sorted by datetime before clustering.  A new cluster starts
    whenever the gap to the previous file exceeds gap_hours.
    """
    if not dated_files:
        return []
    ordered = sorted(dated_files, key=lambda x: x[1])
    gap = datetime.timedelta(hours=gap_hours)
    clusters: list[list[Path]] = [[ordered[0][0]]]
    prev_dt = ordered[0][1]
    for path, dt in ordered[1:]:
        if dt - prev_dt > gap:
            clusters.append([])
        clusters[-1].append(path)
        prev_dt = dt
    return clusters


@dataclass
class Shoot:
    files:    list[Path]
    start:    datetime.datetime
    end:      datetime.datetime
    geo_hint: str | None = None

    @property
    def file_count(self) -> int:
        return len(self.files)


def build_shoots(clusters: list[list[Path]], exif_map: dict[Path, dict]) -> list[Shoot]:
    """Build Shoot objects from file clusters, computing date range and geo hint."""
    shoots: list[Shoot] = []
    for cluster in clusters:
        dates = [d for f in cluster if (d := _best_date(f, exif_map.get(f, {})))]
        if not dates:
            continue
        geo_hint: str | None = None
        for f in cluster:
            data    = exif_map.get(f, {})
            city    = data.get('GeolocationCity', '')
            country = data.get('GeolocationCountry', '')
            if city or country:
                geo_hint = ', '.join(p for p in [city, country] if p)
                break
        shoots.append(Shoot(
            files=cluster, start=min(dates), end=max(dates), geo_hint=geo_hint,
        ))
    return shoots


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def format_shoot_line(index: int, shoot: Shoot) -> str:
    """Format a single shoot summary line for display."""
    geo  = f'   \U0001f4cd {shoot.geo_hint}' if shoot.geo_hint else '   (no GPS)'
    date = shoot.start.strftime('%Y-%m-%d')
    time = f"{shoot.start.strftime('%H:%M')} \u2013 {shoot.end.strftime('%H:%M')}"
    n    = shoot.file_count
    return f'  [{index}]  {date}  {time}   {n} file{"s" if n != 1 else ""}{geo}'


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------

def _unique_dest(dest_dir: Path, name: str) -> Path:
    """Return dest_dir/name, appending a counter if that path already exists."""
    dest = dest_dir / name
    if not dest.exists():
        return dest
    stem, suffix = Path(name).stem, Path(name).suffix
    counter = 1
    while dest.exists():
        dest = dest_dir / f'{stem}_{counter}{suffix}'
        counter += 1
    return dest


def copy_shoot(
    shoot:       Shoot,
    sidecar_map: dict[Path, Path],
    tag:         str,
    dest_root:   Path,
    card_uuid:   str,
    db:          IngestDB,
    dry_run:     bool,
) -> int:
    """Copy a shoot's primary files and their sidecars to dest_root/tag/.

    Returns the total number of files copied (or that would be copied).
    Only primary files are recorded in the import DB; sidecars travel silently.
    """
    dest_dir  = dest_root / tag
    shoot_set = set(shoot.files)
    sidecars  = [sc for sc, primary in sidecar_map.items() if primary in shoot_set]
    total     = 0

    for src in list(shoot.files) + sidecars:
        dest = _unique_dest(dest_dir, src.name)
        if not dry_run:
            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
            if src in shoot_set:
                db.mark_imported(card_uuid, src, tag, dest)
        total += 1

    return total


# ---------------------------------------------------------------------------
# Rename
# ---------------------------------------------------------------------------

def _rename_shoot(dest_dir: Path, tag: str) -> None:
    """Rename files in dest_dir to the YYYYMMDD_HHMMSS_tag_original convention."""
    candidates = [
        p for p in sorted(dest_dir.iterdir())
        if p.is_file()
        and p.suffix.lstrip('.').lower() in (PHOTO_EXTS | VIDEO_EXTS)
        and not ALREADY_RENAMED_RE.match(p.stem)
    ]
    if not candidates:
        return
    print('  Renaming...')
    exif_map = read_exif(candidates)
    for f in candidates:
        dt = _best_date(f, exif_map.get(f, {}))
        if dt is None:
            print(f'  Warning: no date found for {f.name}, skipping rename.', file=sys.stderr)
            continue
        rename_file(f, dt, tag)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='Import photos from an SD card into a local staging directory.',
    )
    p.add_argument('--tag',
                   help='Default tag suggestion (prompted per shoot if omitted)')
    p.add_argument('--execute', action='store_true',
                   help='Copy files (default: dry-run preview)')
    p.add_argument('--no-rename', dest='rename', action='store_false', default=True,
                   help='Skip automatic rename via shoot.py after copying')
    p.add_argument('--incoming', default=str(DEFAULT_INCOMING),
                   help=f'Staging directory root (default: {DEFAULT_INCOMING})')
    p.add_argument('--gap-hours', type=float, default=GAP_HOURS_DEFAULT,
                   help=f'Hours between shoots (default: {GAP_HOURS_DEFAULT})')
    p.add_argument('--db', default=str(DEFAULT_DB_PATH),
                   help=f'Import tracking database (default: {DEFAULT_DB_PATH})')
    p.add_argument('--volumes-dir', default=None, help=argparse.SUPPRESS)
    return p.parse_args(argv)


def _pick_card(cards: list[Path], prompter: Callable[[str], str] = input) -> Path | None:
    """If multiple cards are present, ask the user to choose one."""
    if len(cards) == 1:
        return cards[0]
    print('Multiple SD cards found:')
    for i, c in enumerate(cards, 1):
        print(f'  [{i}] {c.name}')
    raw = prompter('Which card? [1]: ').strip() or '1'
    try:
        return cards[int(raw) - 1]
    except (ValueError, IndexError):
        return None


def main(prompter: Callable[[str], str] = input) -> None:
    args    = parse_args()
    dry_run = not args.execute

    if dry_run:
        print('Dry run — no files will be copied. Pass --execute to apply.\n')

    volumes_dir = Path(args.volumes_dir) if args.volumes_dir else VOLUMES_DIR
    cards = find_cards(volumes_dir)
    if not cards:
        print('No SD card found (no volume with a DCIM folder).')
        sys.exit(1)

    card_path = _pick_card(cards, prompter)
    if card_path is None:
        print('Invalid selection.')
        sys.exit(1)

    print(f'Card: {card_path.name}')
    card_uuid = get_card_uuid(card_path)
    db        = IngestDB(Path(args.db))

    print('Scanning card...')
    primaries, sidecar_map = walk_dcim(card_path)
    new_files = [
        f for f in primaries
        if not db.is_imported(card_uuid, f) and not db.is_ignored(card_uuid, f)
    ]

    if not new_files:
        print('No new files to import.')
        db.close()
        return

    print(f'Reading EXIF from {len(new_files)} files...')
    exif_map = read_exif(new_files)

    dated: list[tuple[Path, datetime.datetime]] = [
        (f, dt) for f in new_files if (dt := _best_date(f, exif_map.get(f, {})))
    ]
    if not dated:
        print('No files with readable dates found.')
        db.close()
        return

    clusters = cluster_by_gap(dated, gap_hours=args.gap_hours)
    shoots   = build_shoots(clusters, exif_map)

    print(f'\nFound {len(shoots)} shoot{"s" if len(shoots) != 1 else ""}:\n')
    for i, shoot in enumerate(shoots, 1):
        print(format_shoot_line(i, shoot))
    print()

    dest_root = Path(args.incoming).expanduser()
    total     = 0

    for i, shoot in enumerate(shoots, 1):
        print(format_shoot_line(i, shoot))
        prompt = '  Tag (or s=skip, i=ignore forever)'
        if args.tag:
            prompt += f' [{args.tag}]'
        prompt += ': '

        response = prompter(prompt).strip()
        if not response and args.tag:
            response = args.tag

        if response.lower() == 'i':
            for f in shoot.files:
                db.mark_ignored(card_uuid, f)
            n = shoot.file_count
            print(f'  Ignored {n} file{"s" if n != 1 else ""}.')  # always persisted, even in dry-run
        elif not response or response.lower() == 's':
            print('  Skipped.')
        else:
            tag = response.lower()
            n   = copy_shoot(shoot, sidecar_map, tag, dest_root, card_uuid, db, dry_run)
            total += n
            dest_dir = dest_root / tag
            if dry_run:
                suffix = ' + rename' if args.rename else ''
                print(f'  Would copy {n} file{"s" if n != 1 else ""} to {dest_dir}/{suffix}')
            else:
                print(f'  Copied {n} file{"s" if n != 1 else ""} to {dest_dir}/')
                if args.rename:
                    _rename_shoot(dest_dir, tag)

    action = 'would be copied' if dry_run else 'copied'
    print(f'\nDone. {total} file{"s" if total != 1 else ""} {action}.')
    db.close()


if __name__ == '__main__':
    main()
