"""Unit tests — pure logic, no filesystem, no exiftool."""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

import fcntl

from organize import (
    DateTier,
    FileCategory,
    _FILENAME_PATTERNS,
    _acquire_lock,
    _date_from_exif_dict,
    _date_from_filename,
    _original_stem,
    build_filename,
    classify,
    dest_dir_for,
    unique_dest,
    walk_source,
    DateResult,
)
from photo import find_xmp, parse_exif_dt as _parse_exif_dt


# ---------------------------------------------------------------------------
# _parse_exif_dt
# ---------------------------------------------------------------------------

class TestParseExifDt:
    def test_valid_basic(self):
        dt = _parse_exif_dt('2023:06:15 10:30:22')
        assert dt == datetime.datetime(2023, 6, 15, 10, 30, 22)

    def test_valid_strips_timezone(self):
        dt = _parse_exif_dt('2023:06:15 10:30:22+07:00')
        assert dt == datetime.datetime(2023, 6, 15, 10, 30, 22)

    def test_valid_strips_negative_timezone(self):
        dt = _parse_exif_dt('2023:06:15 10:30:22-05:00')
        assert dt == datetime.datetime(2023, 6, 15, 10, 30, 22)

    def test_valid_iso8601_T(self):
        dt = _parse_exif_dt('2025-04-30T00:00:00Z')
        assert dt == datetime.datetime(2025, 4, 30, 0, 0, 0)

    def test_valid_iso8601_space(self):
        dt = _parse_exif_dt('2025-04-30 15:45:11')
        assert dt == datetime.datetime(2025, 4, 30, 15, 45, 11)

    def test_valid_iso8601_with_offset(self):
        dt = _parse_exif_dt('2025-05-26T15:45:11+07:00')
        assert dt == datetime.datetime(2025, 5, 26, 15, 45, 11)

    def test_rejects_epoch(self):
        assert _parse_exif_dt('1970:01:01 00:00:00') is None

    def test_empty_string(self):
        assert _parse_exif_dt('') is None

    def test_malformed(self):
        assert _parse_exif_dt('not-a-date') is None


# ---------------------------------------------------------------------------
# _date_from_exif_dict
# ---------------------------------------------------------------------------

class TestDateFromExifDict:
    def test_prefers_datetimeoriginal(self):
        data = {
            'DateTimeOriginal': '2023:06:15 10:30:00',
            'CreateDate':       '2022:01:01 00:00:00',
            'ModifyDate':       '2021:01:01 00:00:00',
        }
        result = _date_from_exif_dict(data)
        assert result is not None
        assert result.dt.year == 2023
        assert result.tier == DateTier.EXIF

    def test_falls_back_to_createdate(self):
        data = {'CreateDate': '2022:03:10 08:00:00'}
        result = _date_from_exif_dict(data)
        assert result is not None
        assert result.dt.year == 2022

    def test_falls_back_to_modifydate(self):
        data = {'ModifyDate': '2021:12:01 12:00:00'}
        result = _date_from_exif_dict(data)
        assert result is not None
        assert result.dt.year == 2021

    def test_returns_none_when_empty(self):
        assert _date_from_exif_dict({}) is None

    def test_returns_none_when_all_missing(self):
        assert _date_from_exif_dict({'Make': 'Apple', 'Model': 'iPhone'}) is None


# ---------------------------------------------------------------------------
# _date_from_filename
# ---------------------------------------------------------------------------

class TestDateFromFilename:
    def _check(self, name: str, year: int, month: int, day: int,
               hour: int = 0, minute: int = 0, second: int = 0) -> None:
        path   = Path(f'{name}.jpg')
        result = _date_from_filename(path)
        assert result is not None, f'No date found in {name!r}'
        assert result.tier == DateTier.FILENAME
        assert result.dt == datetime.datetime(year, month, day, hour, minute, second)

    def test_compact_timestamp(self):
        self._check('20230615_103022', 2023, 6, 15, 10, 30, 22)

    def test_already_renamed(self):
        self._check('20230615_103022_tag_original', 2023, 6, 15, 10, 30, 22)

    def test_iso_date_with_time(self):
        self._check('2023-06-15_10-30-22', 2023, 6, 15, 10, 30, 22)

    def test_iso_date_only(self):
        self._check('2023-06-15_photo', 2023, 6, 15, 0, 0, 0)

    def test_iso_underscore(self):
        self._check('2023_06_15', 2023, 6, 15)

    def test_android_img_prefix(self):
        self._check('IMG_20230615_103022', 2023, 6, 15, 10, 30, 22)

    def test_android_vid_prefix(self):
        self._check('VID_20230615_103022', 2023, 6, 15, 10, 30, 22)

    def test_plain_8digit_date(self):
        self._check('20230615', 2023, 6, 15)

    def test_8digit_embedded_in_name(self):
        self._check('family_20230615_scan', 2023, 6, 15)

    def test_no_match_returns_none(self):
        assert _date_from_filename(Path('IMG_4768.jpg')) is None
        assert _date_from_filename(Path('random_name.jpg')) is None

    def test_rejects_implausible_year(self):
        # 19991315 — month 13 is invalid, should not match
        assert _date_from_filename(Path('19991315.jpg')) is None

    def test_rejects_pre_1980(self):
        assert _date_from_filename(Path('19750101.jpg')) is None


# ---------------------------------------------------------------------------
# build_filename
# ---------------------------------------------------------------------------

class TestBuildFilename:
    _DT = datetime.datetime(2023, 6, 15, 10, 30, 22)

    def test_with_tag(self):
        name = build_filename(self._DT, DateTier.EXIF, 'vacation', 'img0042', 'jpg')
        assert name == '20230615_103022_vacation_img0042.jpg'

    def test_without_tag(self):
        name = build_filename(self._DT, DateTier.EXIF, None, 'img0042', 'jpg')
        assert name == '20230615_103022_img0042.jpg'

    def test_fndate_signal(self):
        name = build_filename(self._DT, DateTier.FILENAME, None, 'img0042', 'jpg')
        assert '_fndate.' in name

    def test_mtime_signal(self):
        name = build_filename(self._DT, DateTier.MTIME, None, 'img0042', 'jpg')
        assert '_mtime.' in name

    def test_xmp_tier_no_signal(self):
        name = build_filename(self._DT, DateTier.XMP, None, 'img0042', 'jpg')
        assert 'fndate' not in name
        assert 'mtime' not in name

    def test_hash6_appended(self):
        name = build_filename(self._DT, DateTier.EXIF, None, 'img0042', 'jpg', hash6='a3f9c1')
        assert name.endswith('_a3f9c1.jpg')

    def test_ext_lowercased(self):
        name = build_filename(self._DT, DateTier.EXIF, None, 'IMG0042', 'RAF')
        assert name.endswith('.raf')

    def test_stem_lowercased_and_stripped(self):
        name = build_filename(self._DT, DateTier.EXIF, None, '_DSF4989', 'raf')
        assert name == '20230615_103022_dsf4989.raf'   # leading _ stripped, no double __


# ---------------------------------------------------------------------------
# dest_dir_for
# ---------------------------------------------------------------------------

class TestDestDirFor:
    _DT   = datetime.datetime(2023, 6, 15, 10, 30, 22)
    _ROOT = Path('/photos')

    def test_photo_normal(self):
        d = dest_dir_for(self._DT, DateTier.EXIF, self._ROOT)
        assert d == Path('/photos/2023/06')

    def test_photo_mtime_goes_undated(self):
        d = dest_dir_for(self._DT, DateTier.MTIME, self._ROOT)
        assert d == Path('/photos/_undated')

    def test_video_normal(self):
        d = dest_dir_for(self._DT, DateTier.EXIF, self._ROOT, is_video=True)
        assert d == Path('/photos/movies/2023/06')

    def test_video_mtime_goes_undated(self):
        d = dest_dir_for(self._DT, DateTier.MTIME, self._ROOT, is_video=True)
        assert d == Path('/photos/movies/_undated')


# ---------------------------------------------------------------------------
# unique_dest
# ---------------------------------------------------------------------------

class TestUniqueDest:
    def test_no_collision(self, tmp_path):
        dest = unique_dest(tmp_path, 'photo.jpg', 'abcdef123456', set())
        assert dest == tmp_path / 'photo.jpg'

    def test_collision_with_claimed(self, tmp_path):
        claimed = {str(tmp_path / 'photo.jpg')}
        dest    = unique_dest(tmp_path, 'photo.jpg', 'abcdef123456', claimed)
        assert dest == tmp_path / 'photo_abcdef.jpg'

    def test_collision_with_existing_file(self, tmp_path):
        (tmp_path / 'photo.jpg').touch()
        dest = unique_dest(tmp_path, 'photo.jpg', 'abcdef123456', set())
        assert dest == tmp_path / 'photo_abcdef.jpg'


# ---------------------------------------------------------------------------
# classify
# ---------------------------------------------------------------------------

class TestClassify:
    def _cls(self, name: str) -> FileCategory:
        return classify(Path(name))

    # Photos
    def test_jpg(self):    assert self._cls('img.jpg')  == FileCategory.PHOTO
    def test_jpeg(self):   assert self._cls('img.JPEG') == FileCategory.PHOTO
    def test_raf(self):    assert self._cls('img.RAF')  == FileCategory.PHOTO
    def test_cr2(self):    assert self._cls('img.cr2')  == FileCategory.PHOTO
    def test_cr3(self):    assert self._cls('img.CR3')  == FileCategory.PHOTO
    def test_dng(self):    assert self._cls('img.dng')  == FileCategory.PHOTO
    def test_heic(self):   assert self._cls('img.HEIC') == FileCategory.PHOTO
    def test_tif(self):    assert self._cls('img.tif')  == FileCategory.PHOTO
    def test_png(self):    assert self._cls('img.png')  == FileCategory.PHOTO
    def test_webp(self):   assert self._cls('img.webp') == FileCategory.PHOTO
    def test_bmp(self):    assert self._cls('img.bmp')  == FileCategory.PHOTO

    # Videos
    def test_mov(self):    assert self._cls('v.mov')  == FileCategory.VIDEO
    def test_mp4(self):    assert self._cls('v.MP4')  == FileCategory.VIDEO
    def test_avi(self):    assert self._cls('v.avi')  == FileCategory.VIDEO
    def test_wmv(self):    assert self._cls('v.wmv')  == FileCategory.VIDEO

    # Sidecars
    def test_xmp(self):    assert self._cls('img.xmp') == FileCategory.XMP
    def test_XMP(self):    assert self._cls('img.XMP') == FileCategory.XMP
    def test_pp3(self):    assert self._cls('img.pp3') == FileCategory.PP3

    # THM
    def test_thm(self):    assert self._cls('vid.THM') == FileCategory.THM

    # Review
    def test_psd(self):    assert self._cls('img.psd')    == FileCategory.REVIEW
    def test_lrcat(self):  assert self._cls('lib.lrcat')  == FileCategory.REVIEW

    # Skip
    def test_lrprev(self):    assert self._cls('img.lrprev')   == FileCategory.SKIP
    def test_ds_store(self):  assert self._cls('.DS_Store')     == FileCategory.SKIP
    def test_ini(self):       assert self._cls('desktop.ini')   == FileCategory.SKIP
    def test_db(self):        assert self._cls('thumbs.db')     == FileCategory.SKIP
    def test_no_ext(self):    assert self._cls('no_extension')  == FileCategory.SKIP
    def test_unknown_ext(self): assert self._cls('file.xyz123') == FileCategory.SKIP


# ---------------------------------------------------------------------------
# _original_stem — already-renamed prefix stripping
# ---------------------------------------------------------------------------

class TestOriginalStem:
    def test_strips_date_time_prefix(self):
        assert _original_stem('20230615_103022_liam06mo_img0042') == 'liam06mo_img0042'

    def test_strips_date_time_no_tag(self):
        assert _original_stem('20230615_103022_img0042') == 'img0042'

    def test_leaves_plain_stem_unchanged(self):
        assert _original_stem('IMG_0042') == 'IMG_0042'

    def test_leaves_partial_match_unchanged(self):
        # Only 6 digits after underscore — not a full timestamp
        assert _original_stem('20230615_1030_img') == '20230615_1030_img'

    def test_leaves_unrelated_stem_unchanged(self):
        assert _original_stem('family_photo_2023') == 'family_photo_2023'


# ---------------------------------------------------------------------------
# find_xmp — double-extension sidecar discovery
# ---------------------------------------------------------------------------

class TestFindXmp:
    def test_finds_standard_xmp(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        xmp = tmp_path / 'photo.xmp'
        xmp.touch()
        assert find_xmp(img) == xmp

    def test_finds_double_extension_xmp(self, tmp_path):
        """Lightroom-style: photo.jpg.xmp alongside photo.jpg"""
        img = tmp_path / 'photo.jpg'
        img.touch()
        xmp = tmp_path / 'photo.jpg.xmp'
        xmp.touch()
        assert find_xmp(img) == xmp

    def test_prefers_standard_over_double_extension(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        standard = tmp_path / 'photo.xmp'
        standard.touch()
        double = tmp_path / 'photo.jpg.xmp'
        double.touch()
        assert find_xmp(img) == standard

    def test_returns_none_when_no_xmp(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        assert find_xmp(img) is None


# ---------------------------------------------------------------------------
# _acquire_lock — concurrent-run prevention
# ---------------------------------------------------------------------------

class TestWalkSource:
    def test_skips_rejected_folder(self, tmp_path: Path):
        keep = tmp_path / 'shoot' / '20260310_090000_liam_dscf0001.jpg'
        reject = tmp_path / 'shoot' / '_Rejected' / '20260310_090000_liam_dscf0002.jpg'
        keep.parent.mkdir(parents=True)
        reject.parent.mkdir(parents=True)
        keep.write_bytes(b'x')
        reject.write_bytes(b'x')
        results = list(walk_source(tmp_path))
        assert keep in results
        assert reject not in results

    def test_walks_normal_subdirs(self, tmp_path: Path):
        f1 = tmp_path / 'a' / 'photo.jpg'
        f2 = tmp_path / 'b' / 'photo.jpg'
        f1.parent.mkdir()
        f2.parent.mkdir()
        f1.write_bytes(b'x')
        f2.write_bytes(b'x')
        results = list(walk_source(tmp_path))
        assert f1 in results
        assert f2 in results


class TestAcquireLock:
    def test_acquires_when_free(self, tmp_path):
        lock = tmp_path / 'test.lock'
        assert _acquire_lock(lock) is True

    def test_blocks_second_acquire(self, tmp_path):
        lock = tmp_path / 'test.lock'
        # Acquire the lock in this process via a separate file handle
        fh = open(lock, 'w')
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # A second acquire attempt on the same lock file should fail
        assert _acquire_lock(lock) is False
        fh.close()

    def test_succeeds_after_lock_released(self, tmp_path):
        lock = tmp_path / 'test.lock'
        fh = open(lock, 'w')
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.close()  # releases the lock
        assert _acquire_lock(lock) is True
