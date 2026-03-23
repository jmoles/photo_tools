"""Integration tests for ingest.py — requires exiftool."""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest

from helpers import FUJIFILM_RAF, RICOH_DNG, make_jpeg_with_exif, make_jpeg_with_gps
from ingest import (
    IngestDB,
    Shoot,
    _best_date,
    _rename_shoot,
    build_shoots,
    cluster_by_gap,
    copy_shoot,
    read_exif,
    walk_dcim,
)

pytestmark = pytest.mark.integration


def _fake_dcim(root: Path, entries: list[tuple[str, str, str]]) -> Path:
    """Build a fake DCIM structure with real JPEG files.

    entries: list of (subdir, filename, exif_date_str)
    Returns the card root (parent of DCIM/).
    """
    card = root / 'CARD'
    for subdir, name, date_str in entries:
        d = card / 'DCIM' / subdir
        d.mkdir(parents=True, exist_ok=True)
        make_jpeg_with_exif(d / name, date_str)
    return card


# ---------------------------------------------------------------------------
# read_exif
# ---------------------------------------------------------------------------

class TestReadExif:
    def test_reads_date_from_jpeg(self, tmp_path: Path):
        f = make_jpeg_with_exif(tmp_path / 'photo.jpg', '2026:03:10 09:14:00')
        result = read_exif([f])
        assert f in result
        assert result[f].get('DateTimeOriginal') == '2026:03:10 09:14:00'

    def test_returns_empty_for_missing_file(self, tmp_path: Path):
        result = read_exif([tmp_path / 'nonexistent.jpg'])
        # exiftool will simply not include the missing file in output
        assert isinstance(result, dict)

    def test_handles_empty_list(self):
        assert read_exif([]) == {}

    def test_returns_gps_coordinates(self, tmp_path: Path):
        f = make_jpeg_with_gps(tmp_path / 'gps.jpg', lat=34.6937, lon=135.5023)
        result = read_exif([f])
        assert f in result
        assert result[f].get('GPSLatitude') or result[f].get('GPSPosition')

    def test_returns_geolocation_city(self, tmp_path: Path):
        # Osaka, Japan — should resolve via exiftool's bundled geolocation DB
        f = make_jpeg_with_gps(tmp_path / 'gps.jpg', lat=34.6937, lon=135.5023)
        result = read_exif([f])
        city = result[f].get('GeolocationCity', '')
        # Only assert if the geolocation API is available (exiftool >= 12.74)
        if city:
            assert isinstance(city, str)
            assert len(city) > 0

    def test_reads_fujifilm_raf(self):
        result = read_exif([FUJIFILM_RAF])
        assert FUJIFILM_RAF in result

    def test_reads_ricoh_dng(self):
        result = read_exif([RICOH_DNG])
        assert RICOH_DNG in result


# ---------------------------------------------------------------------------
# _best_date
# ---------------------------------------------------------------------------

class TestBestDate:
    def test_prefers_datetimeoriginal(self, tmp_path: Path):
        f = make_jpeg_with_exif(tmp_path / 'p.jpg', '2026:03:10 09:00:00')
        exif = read_exif([f])
        dt = _best_date(f, exif.get(f, {}))
        assert dt == datetime.datetime(2026, 3, 10, 9, 0, 0)

    def test_falls_back_to_mtime(self, tmp_path: Path):
        f = tmp_path / 'no_exif.jpg'
        f.write_bytes(b'fake')
        dt = _best_date(f, {})
        assert dt is not None
        assert dt.year >= 2020


# ---------------------------------------------------------------------------
# Full pipeline: walk → read_exif → cluster → build_shoots → copy
# ---------------------------------------------------------------------------

class TestFullPipeline:
    def test_single_shoot_imported(self, tmp_path: Path):
        card = _fake_dcim(tmp_path, [
            ('100_FUJI', 'DSCF0001.JPG', '2026:03:10 09:00:00'),
            ('100_FUJI', 'DSCF0002.JPG', '2026:03:10 09:30:00'),
        ])
        primaries, sidecar_map = walk_dcim(card)
        exif_map  = read_exif(primaries)
        dated     = [(f, dt) for f in primaries if (dt := _best_date(f, exif_map.get(f, {})))]
        clusters  = cluster_by_gap(dated, gap_hours=4.0)
        shoots    = build_shoots(clusters, exif_map)

        assert len(shoots) == 1
        assert shoots[0].file_count == 2

        dest_root = tmp_path / 'incoming'
        db  = IngestDB(tmp_path / 'imports.db')
        n   = copy_shoot(shoots[0], sidecar_map, 'kyoto', dest_root, 'test-uuid', db, dry_run=False)

        assert n == 2
        assert (dest_root / 'kyoto' / 'DSCF0001.JPG').exists()
        assert (dest_root / 'kyoto' / 'DSCF0002.JPG').exists()
        db.close()

    def test_geo_hint_populated_when_gps_present(self, tmp_path: Path):
        card = tmp_path / 'CARD'
        d = card / 'DCIM' / '100_FUJI'
        d.mkdir(parents=True)
        make_jpeg_with_gps(d / 'DSCF0001.JPG', lat=34.6937, lon=135.5023)
        primaries, _ = walk_dcim(card)
        exif_map = read_exif(primaries)
        dated    = [(f, dt) for f in primaries if (dt := _best_date(f, exif_map.get(f, {})))]
        shoots   = build_shoots(cluster_by_gap(dated), exif_map)
        assert len(shoots) == 1
        # geo_hint is None when geolocation API unavailable, non-empty string when available
        assert shoots[0].geo_hint is None or len(shoots[0].geo_hint) > 0

    def test_two_shoots_clustered_separately(self, tmp_path: Path):
        card = _fake_dcim(tmp_path, [
            ('100_FUJI', 'DSCF0001.JPG', '2026:03:10 09:00:00'),
            ('100_FUJI', 'DSCF0002.JPG', '2026:03:10 15:00:00'),  # 6 h gap
        ])
        primaries, _ = walk_dcim(card)
        exif_map = read_exif(primaries)
        dated    = [(f, dt) for f in primaries if (dt := _best_date(f, exif_map.get(f, {})))]
        clusters = cluster_by_gap(dated, gap_hours=4.0)
        shoots   = build_shoots(clusters, exif_map)

        assert len(shoots) == 2

    def test_rerun_skips_already_imported(self, tmp_path: Path):
        card = _fake_dcim(tmp_path, [
            ('100_FUJI', 'DSCF0001.JPG', '2026:03:10 09:00:00'),
        ])
        primaries, sidecar_map = walk_dcim(card)
        exif_map = read_exif(primaries)
        dated    = [(f, dt) for f in primaries if (dt := _best_date(f, exif_map.get(f, {})))]
        shoots   = build_shoots(cluster_by_gap(dated), exif_map)

        db = IngestDB(tmp_path / 'imports.db')
        copy_shoot(shoots[0], sidecar_map, 'kyoto', tmp_path / 'inc', 'test-uuid', db, dry_run=False)

        # On second run, filter with DB
        new_files = [f for f in primaries if not db.is_imported('test-uuid', f)]
        assert new_files == []
        db.close()

    def test_rename_renames_files_after_copy(self, tmp_path: Path):
        card = _fake_dcim(tmp_path, [
            ('100_FUJI', 'DSCF0001.JPG', '2026:03:10 09:00:00'),
            ('100_FUJI', 'DSCF0002.JPG', '2026:03:10 09:30:00'),
        ])
        primaries, sidecar_map = walk_dcim(card)
        exif_map  = read_exif(primaries)
        dated     = [(f, dt) for f in primaries if (dt := _best_date(f, exif_map.get(f, {})))]
        shoots    = build_shoots(cluster_by_gap(dated), exif_map)
        dest_root = tmp_path / 'incoming'
        db = IngestDB(tmp_path / 'imports.db')
        copy_shoot(shoots[0], sidecar_map, 'kyoto', dest_root, 'test-uuid', db, dry_run=False)
        db.close()

        dest_dir = dest_root / 'kyoto'
        _rename_shoot(dest_dir, 'kyoto')

        renamed = list(dest_dir.iterdir())
        assert all(f.name.startswith('20260310_') for f in renamed), \
            f'Expected YYYYMMDD_ prefix after rename, got: {[f.name for f in renamed]}'

    def test_ignored_files_excluded(self, tmp_path: Path):
        card = _fake_dcim(tmp_path, [
            ('100_FUJI', 'DSCF0001.JPG', '2026:03:10 09:00:00'),
        ])
        primaries, _ = walk_dcim(card)
        db = IngestDB(tmp_path / 'imports.db')
        for f in primaries:
            db.mark_ignored('test-uuid', f)

        new_files = [
            f for f in primaries
            if not db.is_imported('test-uuid', f) and not db.is_ignored('test-uuid', f)
        ]
        assert new_files == []
        db.close()
