"""Integration tests — real filesystem, real exiftool."""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from helpers import (
    CANON_CR2, FUJIFILM_RAF, IPHONE_HEIC, IPHONE_PRORAW, RICOH_DNG,
    make_jpeg_no_exif, make_jpeg_with_exif, make_xmp_sidecar,
)
from organize import (
    CacheDB,
    DateTier,
    FileCategory,
    ProcessContext,
    _date_from_xmp,
    build_filename,
    get_date,
    main,
    process_file,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ctx(src: Path, dst: Path, tmp_path: Path, tag: str | None = None, dry_run: bool = False) -> ProcessContext:
    cache = CacheDB(tmp_path / 'cache.db')
    return ProcessContext(
        dest_root=dst,
        source_root=src,
        tag=tag,
        cache=cache,
        hash_index=cache.load_hash_index(),
        claimed_dests=set(),
        claimed_sidecars=set(),
        dry_run=dry_run,
    )


def _run_main(src: Path, dst: Path, tmp_path: Path, extra_args: list[str] | None = None, execute: bool = False) -> None:
    """Invoke main() with standard test args."""
    import sys
    from unittest.mock import patch

    argv = [
        'consolidate.py',
        '--source', str(src),
        '--dest',   str(dst),
        '--hash-cache', str(tmp_path / 'cache.db'),
        '--log',    str(tmp_path / 'test.log'),
    ]
    if execute:
        argv.append('--execute')
    if extra_args:
        argv.extend(extra_args)

    with patch.object(sys, 'argv', argv):
        main()


# ---------------------------------------------------------------------------
# Tier 1 — embedded EXIF
# ---------------------------------------------------------------------------

class TestTier1Exif:
    def test_jpeg_with_exif(self, src, dst, tmp_path):
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:22')
        ctx = _make_ctx(src, dst, tmp_path)
        # Use get_date directly
        from organize import batch_exiftool
        exif = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF
        assert result.dt.year  == 2023
        assert result.dt.month == 6
        assert result.dt.day   == 15

    def test_fujifilm_raf(self, src, dst, tmp_path):
        img = src / FUJIFILM_RAF.name
        shutil.copy2(FUJIFILM_RAF, img)
        from organize import batch_exiftool
        exif   = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF
        assert result.dt == result.dt.replace(year=2026, month=3, day=20, hour=8, minute=5, second=44)

    def test_ricoh_dng(self, src, dst, tmp_path):
        img = src / RICOH_DNG.name
        shutil.copy2(RICOH_DNG, img)
        from organize import batch_exiftool
        exif   = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF
        assert result.dt == result.dt.replace(year=2026, month=3, day=20)

    def test_iphone_heic(self, src, dst, tmp_path):
        img = src / IPHONE_HEIC.name
        shutil.copy2(IPHONE_HEIC, img)
        from organize import batch_exiftool
        exif   = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF

    def test_iphone_proraw_dng(self, src, dst, tmp_path):
        img = src / IPHONE_PRORAW.name
        shutil.copy2(IPHONE_PRORAW, img)
        from organize import batch_exiftool
        exif   = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF

    def test_canon_cr2(self, src, dst, tmp_path):
        img = src / CANON_CR2.name
        shutil.copy2(CANON_CR2, img)
        from organize import batch_exiftool
        exif   = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF
        assert result.dt == result.dt.replace(year=2026, month=3, day=20, hour=8, minute=40, second=20)


# ---------------------------------------------------------------------------
# Tier 2 — XMP sidecar
# ---------------------------------------------------------------------------

class TestTier2XmpSidecar:
    def test_xmp_used_when_no_image_exif(self, src, dst, tmp_path):
        img = make_jpeg_no_exif(src / 'IMG_0001.jpg')
        xmp = make_xmp_sidecar(img, '2023:06:15 10:30:00')
        result = get_date(img, {}, xmp)
        assert result is not None
        assert result.tier == DateTier.XMP
        assert result.dt.year  == 2023
        assert result.dt.month == 6
        assert result.dt.day   == 15

    def test_exif_beats_xmp(self, src, dst, tmp_path):
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2024:01:01 00:00:00')
        xmp = make_xmp_sidecar(img, '2020:01:01 00:00:00')
        result = get_date(img, {'DateTimeOriginal': '2024:01:01 00:00:00'}, xmp)
        assert result is not None
        assert result.tier == DateTier.EXIF
        assert result.dt.year == 2024


# ---------------------------------------------------------------------------
# Tier 3 — filename date
# ---------------------------------------------------------------------------

class TestTier3Filename:
    def test_date_extracted_from_filename(self, src, dst, tmp_path):
        img = make_jpeg_no_exif(src / '20191225_photo.jpg')
        result = get_date(img, {}, None)
        assert result is not None
        assert result.tier == DateTier.FILENAME
        assert result.dt.year  == 2019
        assert result.dt.month == 12
        assert result.dt.day   == 25

    def test_time_zero_when_only_date_in_name(self, src, dst, tmp_path):
        img = make_jpeg_no_exif(src / '20191225_photo.jpg')
        result = get_date(img, {}, None)
        assert result is not None
        assert result.dt.hour   == 0
        assert result.dt.minute == 0
        assert result.dt.second == 0

    def test_fndate_signal_in_output_filename(self, src, dst, tmp_path):
        img = make_jpeg_no_exif(src / '20191225_photo.jpg')
        ctx = _make_ctx(src, dst, tmp_path)
        action = process_file(img, FileCategory.PHOTO, {}, ctx)
        assert action == 'MOVE'
        moved = list((dst / '2019' / '12').glob('*.jpg'))
        assert len(moved) == 1
        assert 'fndate' in moved[0].name


# ---------------------------------------------------------------------------
# Tier 4 — mtime fallback
# ---------------------------------------------------------------------------

class TestTier4Mtime:
    def test_mtime_used_as_last_resort(self, src, dst, tmp_path):
        img = make_jpeg_no_exif(src / 'random_photo.jpg')
        result = get_date(img, {}, None)
        assert result is not None
        assert result.tier == DateTier.MTIME

    def test_mtime_files_go_to_undated(self, src, dst, tmp_path):
        img = make_jpeg_no_exif(src / 'random_photo.jpg')
        ctx = _make_ctx(src, dst, tmp_path)
        process_file(img, FileCategory.PHOTO, {}, ctx)
        moved = list((dst / '_undated').glob('*.jpg'))
        assert len(moved) == 1
        assert 'mtime' in moved[0].name


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_duplicate_goes_to_dupes(self, src, dst, tmp_path):
        img1 = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        img2 = src / 'IMG_0002.jpg'
        shutil.copy2(img1, img2)

        _run_main(src, dst, tmp_path, execute=True)

        moved = list((dst / '2023' / '06').glob('*.jpg'))
        dupes = list((dst / 'dupes').rglob('*.jpg'))
        assert len(moved) == 1
        assert len(dupes) == 1

    def test_duplicate_organized_in_dupes(self, src, dst, tmp_path):
        """Dupes go to dupes/YYYY/MM/, not a flat folder."""
        img1 = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        img2 = src / 'IMG_0002.jpg'
        shutil.copy2(img1, img2)

        _run_main(src, dst, tmp_path, execute=True)

        dupes = list((dst / 'dupes' / '2023' / '06').glob('*.jpg'))
        assert len(dupes) == 1


# ---------------------------------------------------------------------------
# Sidecar handling
# ---------------------------------------------------------------------------

class TestSidecars:
    def test_xmp_travels_with_photo(self, src, dst, tmp_path):
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        make_xmp_sidecar(img)

        _run_main(src, dst, tmp_path, execute=True)

        jpegs = list((dst / '2023' / '06').glob('*.jpg'))
        xmps  = list((dst / '2023' / '06').glob('*.xmp'))
        assert len(jpegs) == 1
        assert len(xmps)  == 1
        assert jpegs[0].stem == xmps[0].stem

    def test_xmp_reference_updated(self, src, dst, tmp_path):
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        xmp = make_xmp_sidecar(img)
        original_name = img.name

        _run_main(src, dst, tmp_path, execute=True)

        xmps = list((dst / '2023' / '06').glob('*.xmp'))
        assert len(xmps) == 1
        content = xmps[0].read_text()
        assert original_name not in content

    def test_pp3_travels_with_photo(self, src, dst, tmp_path):
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        pp3 = img.with_suffix('.pp3')
        pp3.write_text('[Version]\nAppVersion=5.8\n')

        _run_main(src, dst, tmp_path, execute=True)

        pp3s = list((dst / '2023' / '06').glob('*.pp3'))
        assert len(pp3s) == 1

    def test_live_photo_mov_travels_with_heic(self, src, dst, tmp_path):
        if not IPHONE_HEIC.exists():
            pytest.skip('iPhone HEIC sample not available')
        img = src / IPHONE_HEIC.name
        shutil.copy2(IPHONE_HEIC, img)
        mov = img.with_suffix('.mov')
        mov.write_bytes(b'fake mov content for live photo test')

        _run_main(src, dst, tmp_path, execute=True)

        heics = list(dst.rglob('*.heic')) + list(dst.rglob('*.HEIC'))
        movs  = list(dst.rglob('*.mov'))
        assert len(heics) == 1
        assert len(movs)  == 1
        assert heics[0].stem == movs[0].stem

    def test_orphan_xmp_goes_to_review(self, src, dst, tmp_path):
        xmp = src / 'orphan.xmp'
        xmp.write_text('<x:xmpmeta/>')

        _run_main(src, dst, tmp_path, execute=True)

        review_xmps = list((dst / '_review').rglob('*.xmp'))
        assert len(review_xmps) == 1


# ---------------------------------------------------------------------------
# Collision resolution
# ---------------------------------------------------------------------------

class TestCollisions:
    def test_burst_shots_get_hash6_suffix(self, src, dst, tmp_path):
        """Two different files with the same timestamp and same stem → hash6 appended."""
        # Place in separate subdirs so walk yields both despite same name
        sub1 = src / 'a'; sub1.mkdir()
        sub2 = src / 'b'; sub2.mkdir()
        img1 = make_jpeg_with_exif(sub1 / 'shot.jpg', '2023:06:15 10:30:22')
        img2 = make_jpeg_with_exif(sub2 / 'shot.jpg', '2023:06:15 10:30:22')
        # Different content so they are not dupes
        img2.write_bytes(img2.read_bytes() + b'\x00extra')

        _run_main(src, dst, tmp_path, execute=True)

        moved = list((dst / '2023' / '06').glob('*.jpg'))
        assert len(moved) == 2
        names = sorted(m.name for m in moved)
        # Exactly one name has a 6-char hex suffix before .jpg
        import re
        assert sum(1 for n in names if re.search(r'_[0-9a-f]{6}\.jpg$', n)) == 1


# ---------------------------------------------------------------------------
# Dry-run safety
# ---------------------------------------------------------------------------

class TestDryRun:
    def test_no_files_moved_in_dry_run(self, src, dst, tmp_path):
        make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')

        _run_main(src, dst, tmp_path, execute=False)

        assert not any(dst.rglob('*.jpg'))
        assert list(src.glob('*.jpg'))   # source unchanged

    def test_no_cache_written_in_dry_run(self, src, dst, tmp_path):
        make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        cache_path = tmp_path / 'cache.db'

        _run_main(src, dst, tmp_path, execute=False)

        # Cache file may exist (SQLite created on open) but must have no processed rows
        if cache_path.exists():
            conn = sqlite3.connect(cache_path)
            count = conn.execute('SELECT COUNT(*) FROM processed').fetchone()[0]
            conn.close()
            assert count == 0


# ---------------------------------------------------------------------------
# Resume / cache
# ---------------------------------------------------------------------------

class TestResume:
    def test_cached_files_not_rehashed(self, src, dst, tmp_path):
        """Files already in cache+dest are skipped on second run."""
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')

        # First run
        _run_main(src, dst, tmp_path, execute=True)
        moved = list((dst / '2023' / '06').glob('*.jpg'))
        assert len(moved) == 1

        # Remove source, leave dest — second run should hit cache
        # (source gone but cache+dest still valid)
        log_path = tmp_path / 'test.log'
        log_path.write_text('')   # reset log

        _run_main(src, dst, tmp_path, execute=True)
        # No errors in log
        log_content = log_path.read_text()
        assert 'ERROR' not in log_content

    def test_stale_cache_entry_reprocessed(self, src, dst, tmp_path):
        """If dest file is removed, source is reprocessed even if fingerprint matches."""
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')

        _run_main(src, dst, tmp_path, execute=True)
        moved = list((dst / '2023' / '06').glob('*.jpg'))
        assert len(moved) == 1

        # Delete the dest file to simulate partial failure
        moved[0].unlink()
        # Put source back
        img = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')

        _run_main(src, dst, tmp_path, execute=True)
        moved = list((dst / '2023' / '06').glob('*.jpg'))
        assert len(moved) == 1


# ---------------------------------------------------------------------------
# Already-renamed files
# ---------------------------------------------------------------------------

class TestAlreadyRenamed:
    def test_exif_date_takes_priority_over_filename(self, src, dst, tmp_path):
        """File named 20200101_…jpg but EXIF says 2023 → organised under 2023."""
        img = make_jpeg_with_exif(src / '20200101_000000_old_photo.jpg', '2023:06:15 10:30:00')
        ctx = _make_ctx(src, dst, tmp_path)
        from organize import batch_exiftool
        exif   = batch_exiftool([img])
        result = get_date(img, exif.get(img, {}), None)
        assert result is not None
        assert result.tier == DateTier.EXIF
        assert result.dt.year == 2023


# ---------------------------------------------------------------------------
# Symlink handling
# ---------------------------------------------------------------------------

class TestSymlinks:
    def test_symlinked_file_processed_once(self, src, dst, tmp_path):
        img  = make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')
        link = src / 'IMG_link.jpg'
        link.symlink_to(img)

        _run_main(src, dst, tmp_path, execute=True)

        moved = list((dst / '2023' / '06').glob('*.jpg'))
        dupes = list((dst / 'dupes').rglob('*.jpg'))
        # walk_source deduplicates by real path — only one copy processed, no dupe
        assert len(moved) == 1
        assert len(dupes) == 0


# ---------------------------------------------------------------------------
# THM files
# ---------------------------------------------------------------------------

class TestThmFiles:
    def test_paired_thm_discarded(self, src, dst, tmp_path):
        img = make_jpeg_with_exif(src / 'MVI_0001.jpg', '2023:06:15 10:30:00')
        thm = src / 'MVI_0001.THM'
        thm.write_bytes(b'fake thumbnail')

        _run_main(src, dst, tmp_path, execute=True)

        assert not thm.exists()

    def test_unpaired_thm_to_review(self, src, dst, tmp_path):
        thm = src / 'MVI_9999.THM'
        thm.write_bytes(b'fake thumbnail')

        _run_main(src, dst, tmp_path, execute=True)

        review = list((dst / '_review').rglob('*.THM')) + list((dst / '_review').rglob('*.thm'))
        assert len(review) == 1


# ---------------------------------------------------------------------------
# Review files
# ---------------------------------------------------------------------------

class TestReviewFiles:
    def test_psd_goes_to_review(self, src, dst, tmp_path):
        psd = src / 'design.psd'
        psd.write_bytes(b'fake psd')

        _run_main(src, dst, tmp_path, execute=True)

        review = list((dst / '_review').rglob('*.psd'))
        assert len(review) == 1

    def test_lrprev_skipped(self, src, dst, tmp_path):
        lr = src / 'catalog.lrprev'
        lr.write_bytes(b'fake preview')

        _run_main(src, dst, tmp_path, execute=True)

        assert not list(dst.rglob('*.lrprev'))
        assert not list((dst / '_review').rglob('*.lrprev'))


# ---------------------------------------------------------------------------
# Video routing
# ---------------------------------------------------------------------------

class TestVideoRouting:
    def test_mp4_goes_to_movies(self, src, dst, tmp_path):
        mp4 = src / 'clip_20230615.mp4'
        mp4.write_bytes(b'fake video content for test')

        _run_main(src, dst, tmp_path, execute=True)

        movies = list((dst / 'movies').rglob('*.mp4'))
        assert len(movies) == 1


# ---------------------------------------------------------------------------
# Tag omission
# ---------------------------------------------------------------------------

class TestTagOmission:
    def test_no_tag_segment_when_tag_absent(self, src, dst, tmp_path):
        make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')

        _run_main(src, dst, tmp_path, execute=True)

        moved = list((dst / '2023' / '06').glob('*.jpg'))
        assert len(moved) == 1
        # No tag segment: name starts with date_time_ and contains no tag keyword
        assert moved[0].name.startswith('20230615_103000_')
        assert 'vacation' not in moved[0].name

    def test_tag_present_when_provided(self, src, dst, tmp_path):
        make_jpeg_with_exif(src / 'IMG_0001.jpg', '2023:06:15 10:30:00')

        _run_main(src, dst, tmp_path, execute=True, extra_args=['--tag', 'vacation'])

        moved = list((dst / '2023' / '06').glob('*.jpg'))
        assert len(moved) == 1
        assert 'vacation' in moved[0].name
