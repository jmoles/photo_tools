"""Unit tests for ingest.py — pure logic and filesystem, no exiftool required."""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from ingest import (
    IngestDB,
    Shoot,
    _best_date,
    _pick_card,
    _unique_dest,
    build_shoots,
    cluster_by_gap,
    copy_shoot,
    find_cards,
    format_shoot_line,
    parse_args,
    walk_dcim,
)


# ---------------------------------------------------------------------------
# cluster_by_gap
# ---------------------------------------------------------------------------

class TestClusterByGap:
    def test_empty(self):
        assert cluster_by_gap([]) == []

    def test_single_file(self):
        p  = Path('a.jpg')
        dt = datetime.datetime(2026, 3, 10, 9, 0)
        assert cluster_by_gap([(p, dt)]) == [[p]]

    def test_two_files_within_gap(self):
        p1, p2 = Path('a.jpg'), Path('b.jpg')
        dt1 = datetime.datetime(2026, 3, 10, 9, 0)
        dt2 = datetime.datetime(2026, 3, 10, 11, 0)   # 2 h — within 4 h gap
        assert cluster_by_gap([(p1, dt1), (p2, dt2)], gap_hours=4.0) == [[p1, p2]]

    def test_two_files_across_gap(self):
        p1, p2 = Path('a.jpg'), Path('b.jpg')
        dt1 = datetime.datetime(2026, 3, 10, 9, 0)
        dt2 = datetime.datetime(2026, 3, 10, 14, 1)   # 5 h 1 m — exceeds gap
        assert cluster_by_gap([(p1, dt1), (p2, dt2)], gap_hours=4.0) == [[p1], [p2]]

    def test_exact_boundary_is_same_cluster(self):
        p1, p2 = Path('a.jpg'), Path('b.jpg')
        dt1 = datetime.datetime(2026, 3, 10, 9, 0)
        dt2 = datetime.datetime(2026, 3, 10, 13, 0)   # exactly 4 h — not exceeded
        assert cluster_by_gap([(p1, dt1), (p2, dt2)], gap_hours=4.0) == [[p1, p2]]

    def test_sorts_unsorted_input(self):
        p1, p2, p3 = Path('a.jpg'), Path('b.jpg'), Path('c.jpg')
        dt1 = datetime.datetime(2026, 3, 10, 10, 0)
        dt2 = datetime.datetime(2026, 3, 10, 11, 0)
        dt3 = datetime.datetime(2026, 3, 10,  8, 0)   # earliest, given last
        result = cluster_by_gap([(p1, dt1), (p2, dt2), (p3, dt3)], gap_hours=4.0)
        assert result == [[p3, p1, p2]]

    def test_three_clusters(self):
        files = [Path(f'{i}.jpg') for i in range(6)]
        dts   = [
            datetime.datetime(2026, 3, 10,  9, 0),
            datetime.datetime(2026, 3, 10, 10, 0),
            datetime.datetime(2026, 3, 10, 15, 0),   # +5 h gap
            datetime.datetime(2026, 3, 10, 16, 0),
            datetime.datetime(2026, 3, 10, 22, 0),   # +6 h gap
            datetime.datetime(2026, 3, 10, 23, 0),
        ]
        result = cluster_by_gap(list(zip(files, dts)), gap_hours=4.0)
        assert len(result) == 3
        assert result[0] == [files[0], files[1]]
        assert result[1] == [files[2], files[3]]
        assert result[2] == [files[4], files[5]]

    def test_custom_gap(self):
        p1, p2 = Path('a.jpg'), Path('b.jpg')
        dt1 = datetime.datetime(2026, 3, 10, 9, 0)
        dt2 = datetime.datetime(2026, 3, 10, 10, 0)   # 1 h apart
        # 2 h gap → same cluster
        assert cluster_by_gap([(p1, dt1), (p2, dt2)], gap_hours=2.0) == [[p1, p2]]
        # 0.5 h gap → separate clusters
        assert cluster_by_gap([(p1, dt1), (p2, dt2)], gap_hours=0.5) == [[p1], [p2]]


# ---------------------------------------------------------------------------
# format_shoot_line
# ---------------------------------------------------------------------------

class TestFormatShootLine:
    def _shoot(self, n: int = 1, geo: str | None = None) -> Shoot:
        return Shoot(
            files=[Path(f'{i}.jpg') for i in range(n)],
            start=datetime.datetime(2026, 3, 10, 9, 14),
            end=datetime.datetime(2026, 3, 10, 12, 43),
            geo_hint=geo,
        )

    def test_contains_index(self):
        assert '[3]' in format_shoot_line(3, self._shoot())

    def test_contains_date(self):
        assert '2026-03-10' in format_shoot_line(1, self._shoot())

    def test_contains_time_range(self):
        line = format_shoot_line(1, self._shoot())
        assert '09:14' in line
        assert '12:43' in line

    def test_geo_hint_shown(self):
        line = format_shoot_line(1, self._shoot(geo='Osaka, Japan'))
        assert 'Osaka, Japan' in line
        assert '(no GPS)' not in line

    def test_no_gps_shown(self):
        line = format_shoot_line(1, self._shoot(geo=None))
        assert '(no GPS)' in line

    def test_singular_file_count(self):
        line = format_shoot_line(1, self._shoot(n=1))
        assert '1 file' in line
        assert '1 files' not in line

    def test_plural_file_count(self):
        line = format_shoot_line(1, self._shoot(n=5))
        assert '5 files' in line


# ---------------------------------------------------------------------------
# find_cards
# ---------------------------------------------------------------------------

class TestFindCards:
    def test_empty_volumes(self, tmp_path: Path):
        assert find_cards(tmp_path) == []

    def test_volume_without_dcim_ignored(self, tmp_path: Path):
        (tmp_path / 'SANDISK').mkdir()
        assert find_cards(tmp_path) == []

    def test_volume_with_dcim_detected(self, tmp_path: Path):
        card = tmp_path / 'FUJIFILM'
        (card / 'DCIM').mkdir(parents=True)
        assert find_cards(tmp_path) == [card]

    def test_multiple_cards_returned(self, tmp_path: Path):
        for name in ('FUJIFILM', 'RICOH'):
            (tmp_path / name / 'DCIM').mkdir(parents=True)
        cards = find_cards(tmp_path)
        assert len(cards) == 2
        assert {c.name for c in cards} == {'FUJIFILM', 'RICOH'}

    def test_file_named_dcim_ignored(self, tmp_path: Path):
        card = tmp_path / 'WEIRDCARD'
        card.mkdir()
        (card / 'DCIM').write_text('not a dir')
        assert find_cards(tmp_path) == []


# ---------------------------------------------------------------------------
# walk_dcim
# ---------------------------------------------------------------------------

class TestWalkDcim:
    def _card(self, tmp_path: Path) -> Path:
        card = tmp_path / 'FUJIFILM'
        (card / 'DCIM' / '100_FUJI').mkdir(parents=True)
        return card

    def test_finds_jpg(self, tmp_path: Path):
        card = self._card(tmp_path)
        f = card / 'DCIM' / '100_FUJI' / 'DSCF0001.JPG'
        f.write_bytes(b'fake')
        primaries, _ = walk_dcim(card)
        assert primaries == [f]

    def test_finds_raf(self, tmp_path: Path):
        card = self._card(tmp_path)
        f = card / 'DCIM' / '100_FUJI' / 'DSCF0001.RAF'
        f.write_bytes(b'fake')
        primaries, _ = walk_dcim(card)
        assert primaries == [f]

    def test_ignores_system_files(self, tmp_path: Path):
        card = self._card(tmp_path)
        (card / 'DCIM' / '100_FUJI' / '.DS_Store').write_bytes(b'x')
        (card / 'DCIM' / '100_FUJI' / 'DSCF0001.JPG').write_bytes(b'x')
        primaries, _ = walk_dcim(card)
        assert len(primaries) == 1

    def test_maps_xmp_sidecar(self, tmp_path: Path):
        card = self._card(tmp_path)
        jpg = card / 'DCIM' / '100_FUJI' / 'DSCF0001.JPG'
        xmp = card / 'DCIM' / '100_FUJI' / 'DSCF0001.xmp'
        jpg.write_bytes(b'fake')
        xmp.write_text('<x:xmpmeta/>')
        primaries, sidecar_map = walk_dcim(card)
        assert xmp in sidecar_map
        assert sidecar_map[xmp] == jpg

    def test_maps_double_extension_xmp(self, tmp_path: Path):
        card = self._card(tmp_path)
        jpg = card / 'DCIM' / '100_FUJI' / 'DSCF0001.JPG'
        xmp = card / 'DCIM' / '100_FUJI' / 'DSCF0001.JPG.xmp'
        jpg.write_bytes(b'fake')
        xmp.write_text('<x:xmpmeta/>')
        _, sidecar_map = walk_dcim(card)
        assert xmp in sidecar_map

    def test_walks_multiple_subdirs(self, tmp_path: Path):
        card = tmp_path / 'FUJIFILM'
        for sub in ('100_FUJI', '101_FUJI'):
            d = card / 'DCIM' / sub
            d.mkdir(parents=True)
            (d / f'DSCF000{sub[0]}.JPG').write_bytes(b'fake')
        primaries, _ = walk_dcim(card)
        assert len(primaries) == 2


# ---------------------------------------------------------------------------
# IngestDB
# ---------------------------------------------------------------------------

class TestIngestDB:
    def test_not_imported_initially(self, tmp_path: Path):
        db = IngestDB(tmp_path / 'imports.db')
        f  = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        assert not db.is_imported('uuid-1', f)
        db.close()

    def test_mark_and_check_imported(self, tmp_path: Path):
        db   = IngestDB(tmp_path / 'imports.db')
        f    = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        dest = tmp_path / 'dest' / 'test.jpg'
        db.mark_imported('uuid-1', f, 'mytag', dest)
        assert db.is_imported('uuid-1', f)
        db.close()

    def test_imported_is_card_specific(self, tmp_path: Path):
        db   = IngestDB(tmp_path / 'imports.db')
        f    = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        dest = tmp_path / 'dest' / 'test.jpg'
        db.mark_imported('uuid-1', f, 'mytag', dest)
        assert not db.is_imported('uuid-2', f)
        db.close()

    def test_mark_imported_idempotent(self, tmp_path: Path):
        db   = IngestDB(tmp_path / 'imports.db')
        f    = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        dest = tmp_path / 'dest' / 'test.jpg'
        db.mark_imported('uuid-1', f, 'tag1', dest)
        db.mark_imported('uuid-1', f, 'tag2', dest)   # second call should not raise
        assert db.is_imported('uuid-1', f)
        db.close()

    def test_not_ignored_initially(self, tmp_path: Path):
        db = IngestDB(tmp_path / 'imports.db')
        f  = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        assert not db.is_ignored('uuid-1', f)
        db.close()

    def test_mark_and_check_ignored(self, tmp_path: Path):
        db = IngestDB(tmp_path / 'imports.db')
        f  = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        db.mark_ignored('uuid-1', f)
        assert db.is_ignored('uuid-1', f)
        db.close()

    def test_ignored_is_card_specific(self, tmp_path: Path):
        db = IngestDB(tmp_path / 'imports.db')
        f  = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        db.mark_ignored('uuid-1', f)
        assert not db.is_ignored('uuid-2', f)
        db.close()

    def test_size_change_invalidates_record(self, tmp_path: Path):
        db   = IngestDB(tmp_path / 'imports.db')
        f    = tmp_path / 'test.jpg'
        f.write_bytes(b'x')
        dest = tmp_path / 'dest' / 'test.jpg'
        db.mark_imported('uuid-1', f, 'tag', dest)
        f.write_bytes(b'different content')   # size changed
        assert not db.is_imported('uuid-1', f)
        db.close()


# ---------------------------------------------------------------------------
# copy_shoot
# ---------------------------------------------------------------------------

class TestCopyShoot:
    def _make_shoot(self, files: list[Path]) -> Shoot:
        dt = datetime.datetime(2026, 3, 10, 9, 0)
        return Shoot(files=files, start=dt, end=dt)

    def test_copies_files(self, tmp_path: Path):
        src = tmp_path / 'card' / 'DCIM' / '100_FUJI'
        src.mkdir(parents=True)
        f   = src / 'DSCF0001.JPG'
        f.write_bytes(b'photo data')
        dest_root = tmp_path / 'incoming'
        db  = IngestDB(tmp_path / 'imports.db')
        n   = copy_shoot(self._make_shoot([f]), {}, 'tokyo', dest_root, 'uuid-1', db, dry_run=False)
        assert n == 1
        assert (dest_root / 'tokyo' / 'DSCF0001.JPG').exists()
        db.close()

    def test_dry_run_does_not_copy(self, tmp_path: Path):
        src = tmp_path / 'src'
        src.mkdir()
        f   = src / 'DSCF0001.JPG'
        f.write_bytes(b'photo data')
        dest_root = tmp_path / 'incoming'
        db  = IngestDB(tmp_path / 'imports.db')
        copy_shoot(self._make_shoot([f]), {}, 'tokyo', dest_root, 'uuid-1', db, dry_run=True)
        assert not (dest_root / 'tokyo').exists()
        db.close()

    def test_dry_run_does_not_record_import(self, tmp_path: Path):
        src = tmp_path / 'src'
        src.mkdir()
        f   = src / 'DSCF0001.JPG'
        f.write_bytes(b'photo data')
        db  = IngestDB(tmp_path / 'imports.db')
        copy_shoot(self._make_shoot([f]), {}, 'tokyo', tmp_path / 'inc', 'uuid-1', db, dry_run=True)
        assert not db.is_imported('uuid-1', f)
        db.close()

    def test_marks_imported_in_db(self, tmp_path: Path):
        src = tmp_path / 'src'
        src.mkdir()
        f   = src / 'DSCF0001.JPG'
        f.write_bytes(b'photo data')
        db  = IngestDB(tmp_path / 'imports.db')
        copy_shoot(self._make_shoot([f]), {}, 'tokyo', tmp_path / 'inc', 'uuid-1', db, dry_run=False)
        assert db.is_imported('uuid-1', f)
        db.close()

    def test_copies_sidecar_alongside(self, tmp_path: Path):
        src = tmp_path / 'src'
        src.mkdir()
        f   = src / 'DSCF0001.JPG'
        xmp = src / 'DSCF0001.xmp'
        f.write_bytes(b'photo data')
        xmp.write_text('<x:xmpmeta/>')
        dest_root = tmp_path / 'incoming'
        db  = IngestDB(tmp_path / 'imports.db')
        n   = copy_shoot(
            self._make_shoot([f]), {xmp: f}, 'tokyo', dest_root, 'uuid-1', db, dry_run=False,
        )
        assert n == 2
        assert (dest_root / 'tokyo' / 'DSCF0001.xmp').exists()
        db.close()

    def test_sidecar_not_recorded_as_imported(self, tmp_path: Path):
        src = tmp_path / 'src'
        src.mkdir()
        f   = src / 'DSCF0001.JPG'
        xmp = src / 'DSCF0001.xmp'
        f.write_bytes(b'photo data')
        xmp.write_text('<x:xmpmeta/>')
        db  = IngestDB(tmp_path / 'imports.db')
        copy_shoot(
            self._make_shoot([f]), {xmp: f}, 'tokyo', tmp_path / 'inc', 'uuid-1', db, dry_run=False,
        )
        assert db.is_imported('uuid-1', f)
        assert not db.is_imported('uuid-1', xmp)
        db.close()

    def test_name_collision_resolved(self, tmp_path: Path):
        src = tmp_path / 'src'
        src.mkdir()
        f1 = src / 'DSCF0001.JPG'
        f2 = src / 'DSCF0001.JPG'   # same name — handled via _unique_dest
        f1.write_bytes(b'photo one')
        dest_root = tmp_path / 'incoming' / 'tokyo'
        dest_root.mkdir(parents=True)
        (dest_root / 'DSCF0001.JPG').write_bytes(b'already here')
        db = IngestDB(tmp_path / 'imports.db')
        copy_shoot(self._make_shoot([f1]), {}, 'tokyo', tmp_path / 'incoming', 'uuid-1', db, dry_run=False)
        # Both the original and the renamed copy should exist
        assert (dest_root / 'DSCF0001.JPG').exists()
        assert (dest_root / 'DSCF0001_1.JPG').exists()
        db.close()


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_rename_on_by_default(self):
        args = parse_args([])
        assert args.rename is True

    def test_no_rename_flag_disables_rename(self):
        args = parse_args(['--no-rename'])
        assert args.rename is False


# ---------------------------------------------------------------------------
# _unique_dest
# ---------------------------------------------------------------------------

class TestUniqueDest:
    def test_no_collision(self, tmp_path: Path):
        assert _unique_dest(tmp_path, 'photo.jpg') == tmp_path / 'photo.jpg'

    def test_collision_appends_counter(self, tmp_path: Path):
        (tmp_path / 'photo.jpg').write_bytes(b'x')
        assert _unique_dest(tmp_path, 'photo.jpg') == tmp_path / 'photo_1.jpg'

    def test_multiple_collisions(self, tmp_path: Path):
        (tmp_path / 'photo.jpg').write_bytes(b'x')
        (tmp_path / 'photo_1.jpg').write_bytes(b'x')
        assert _unique_dest(tmp_path, 'photo.jpg') == tmp_path / 'photo_2.jpg'


# ---------------------------------------------------------------------------
# _pick_card
# ---------------------------------------------------------------------------

class TestPickCard:
    def test_single_card_returned_without_prompt(self):
        card = Path('/Volumes/FUJIFILM')
        called = []
        result = _pick_card([card], prompter=lambda _: called.append(1) or '1')
        assert result == card
        assert called == []  # prompter never invoked for single card

    def test_multiple_cards_selects_by_number(self):
        cards = [Path('/Volumes/A'), Path('/Volumes/B')]
        result = _pick_card(cards, prompter=lambda _: '2')
        assert result == cards[1]

    def test_empty_input_defaults_to_first(self):
        cards = [Path('/Volumes/A'), Path('/Volumes/B')]
        result = _pick_card(cards, prompter=lambda _: '')
        assert result == cards[0]

    def test_invalid_number_returns_none(self):
        cards = [Path('/Volumes/A'), Path('/Volumes/B')]
        assert _pick_card(cards, prompter=lambda _: '9') is None

    def test_non_numeric_returns_none(self):
        cards = [Path('/Volumes/A'), Path('/Volumes/B')]
        assert _pick_card(cards, prompter=lambda _: 'foo') is None


# ---------------------------------------------------------------------------
# _best_date
# ---------------------------------------------------------------------------

class TestBestDate:
    def test_prefers_datetimeoriginal(self, tmp_path):
        f = tmp_path / 'photo.jpg'
        f.write_bytes(b'x')
        exif = {'DateTimeOriginal': '2026:03:10 09:14:00', 'ModifyDate': '2020:01:01 00:00:00'}
        dt = _best_date(f, exif)
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 3

    def test_falls_back_to_mtime(self, tmp_path):
        f = tmp_path / 'photo.jpg'
        f.write_bytes(b'x')
        dt = _best_date(f, {})
        assert dt is not None  # mtime always available for a real file

    def test_returns_none_for_missing_file(self, tmp_path):
        f = tmp_path / 'nonexistent.jpg'
        assert _best_date(f, {}) is None


# ---------------------------------------------------------------------------
# build_shoots
# ---------------------------------------------------------------------------

class TestBuildShoots:
    _DT = datetime.datetime(2026, 3, 10, 9, 14)

    def test_empty_clusters_returns_empty(self):
        assert build_shoots([], {}) == []

    def test_undatable_cluster_excluded(self, tmp_path):
        """A cluster with no datable files must be silently dropped."""
        f = tmp_path / 'photo.jpg'
        f.write_bytes(b'x')
        # No EXIF and file doesn't exist → _best_date returns None
        shoots = build_shoots([[Path('/nonexistent/ghost.jpg')]], {})
        assert shoots == []

    def test_single_cluster_becomes_shoot(self, tmp_path):
        f = tmp_path / 'photo.jpg'
        f.write_bytes(b'x')
        exif_map = {f: {'DateTimeOriginal': '2026:03:10 09:14:00'}}
        shoots = build_shoots([[f]], exif_map)
        assert len(shoots) == 1
        assert shoots[0].start == datetime.datetime(2026, 3, 10, 9, 14, 0)

    def test_geo_hint_extracted(self, tmp_path):
        f = tmp_path / 'photo.jpg'
        f.write_bytes(b'x')
        exif_map = {f: {
            'DateTimeOriginal':    '2026:03:10 09:14:00',
            'GeolocationCity':    'Osaka',
            'GeolocationCountry': 'Japan',
        }}
        shoots = build_shoots([[f]], exif_map)
        assert shoots[0].geo_hint == 'Osaka, Japan'

    def test_no_geo_hint_when_absent(self, tmp_path):
        f = tmp_path / 'photo.jpg'
        f.write_bytes(b'x')
        exif_map = {f: {'DateTimeOriginal': '2026:03:10 09:14:00'}}
        shoots = build_shoots([[f]], exif_map)
        assert shoots[0].geo_hint is None
