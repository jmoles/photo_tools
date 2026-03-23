"""Unit tests for rename.py — pure logic, filesystem via tmp_path, exifread mocked."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from shoot import check_already_renamed, parse_args, process_file
from photo import ALREADY_RENAMED_RE, find_xmp, rename_xmp


# ---------------------------------------------------------------------------
# find_xmp
# ---------------------------------------------------------------------------

class TestFindXmp:
    def test_finds_lowercase_xmp(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        xmp = tmp_path / 'photo.xmp'
        xmp.touch()
        assert find_xmp(img) == xmp

    def test_finds_uppercase_xmp(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        xmp = tmp_path / 'photo.XMP'
        xmp.touch()
        result = find_xmp(img)
        # Use samefile() — on case-insensitive filesystems (macOS) photo.XMP
        # and photo.xmp are the same inode, so path equality is unreliable.
        assert result is not None and result.samefile(xmp)

    def test_prefers_lowercase_over_uppercase(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        lower = tmp_path / 'photo.xmp'
        lower.touch()
        upper = tmp_path / 'photo.XMP'
        upper.touch()
        assert find_xmp(img) == lower

    def test_returns_none_when_absent(self, tmp_path):
        img = tmp_path / 'photo.jpg'
        img.touch()
        assert find_xmp(img) is None


# ---------------------------------------------------------------------------
# rename_xmp
# ---------------------------------------------------------------------------

class TestRenameXmp:
    def test_renames_file(self, tmp_path):
        xmp = tmp_path / 'photo.xmp'
        xmp.write_text('<xmp>photo.jpg</xmp>')
        new_xmp = tmp_path / 'renamed.xmp'
        rename_xmp(xmp, new_xmp, 'photo.jpg', 'renamed.jpg')
        assert new_xmp.exists()
        assert not xmp.exists()

    def test_updates_filename_reference(self, tmp_path):
        xmp = tmp_path / 'photo.xmp'
        xmp.write_text('<xmp filename="photo.jpg">stuff</xmp>')
        new_xmp = tmp_path / 'renamed.xmp'
        rename_xmp(xmp, new_xmp, 'photo.jpg', 'renamed.jpg')
        assert 'renamed.jpg' in new_xmp.read_text()
        assert 'photo.jpg' not in new_xmp.read_text()

    def test_deletes_original(self, tmp_path):
        xmp = tmp_path / 'photo.xmp'
        xmp.write_text('content')
        rename_xmp(xmp, tmp_path / 'new.xmp', 'photo.jpg', 'new.jpg')
        assert not xmp.exists()

    def test_write_failure_preserves_original(self, tmp_path):
        """If writing the new XMP fails, the original must not be deleted."""
        xmp = tmp_path / 'photo.xmp'
        xmp.write_text('<xmp>photo.jpg</xmp>')
        new_xmp = tmp_path / 'renamed.xmp'
        with patch('pathlib.Path.write_text', side_effect=OSError('disk full')):
            with pytest.raises(OSError):
                rename_xmp(xmp, new_xmp, 'photo.jpg', 'renamed.jpg')
        assert xmp.exists(), 'original XMP must be preserved after write failure'

    def test_write_failure_cleans_up_partial_new_file(self, tmp_path):
        """A pre-existing file at the new path must be removed on write failure.

        Simulates: new_xmp already exists from a previous partial run, then
        write_text raises (e.g. disk full mid-write). The cleanup code must
        unlink it so the next run doesn't see a corrupt sidecar.
        """
        xmp = tmp_path / 'photo.xmp'
        xmp.write_text('<xmp>photo.jpg</xmp>')
        new_xmp = tmp_path / 'renamed.xmp'
        new_xmp.write_bytes(b'corrupt partial')  # pre-existing partial file
        with patch('pathlib.Path.write_text', side_effect=OSError('disk full')):
            with pytest.raises(OSError):
                rename_xmp(xmp, new_xmp, 'photo.jpg', 'renamed.jpg')
        assert not new_xmp.exists(), 'partial new XMP must be cleaned up after write failure'
        assert xmp.exists(), 'original XMP must survive'


# ---------------------------------------------------------------------------
# ALREADY_RENAMED_RE
# ---------------------------------------------------------------------------

class TestAlreadyRenamedRe:
    def test_matches_full_pattern(self):
        assert ALREADY_RENAMED_RE.match('20230615_103022_tag_img0042')

    def test_matches_no_tag(self):
        assert ALREADY_RENAMED_RE.match('20230615_103022_img0042')

    def test_no_match_plain(self):
        assert not ALREADY_RENAMED_RE.match('IMG_0042')

    def test_no_match_partial_date(self):
        assert not ALREADY_RENAMED_RE.match('20230615_1030_img')


# ---------------------------------------------------------------------------
# check_already_renamed
# ---------------------------------------------------------------------------

class TestCheckAlreadyRenamed:
    def test_detects_renamed_photo(self, tmp_path):
        (tmp_path / '20230615_103022_tag_img0042.jpg').touch()
        result = check_already_renamed(tmp_path)
        assert '20230615_103022_tag_img0042.jpg' in result

    def test_ignores_plain_photo(self, tmp_path):
        (tmp_path / 'IMG_0042.jpg').touch()
        assert check_already_renamed(tmp_path) == []

    def test_ignores_non_photo_files(self, tmp_path):
        (tmp_path / '20230615_103022_tag_file.txt').touch()
        (tmp_path / '20230615_103022_tag_file.xmp').touch()
        assert check_already_renamed(tmp_path) == []

    def test_returns_multiple(self, tmp_path):
        (tmp_path / '20230615_103022_tag_img0001.cr2').touch()
        (tmp_path / '20230615_103023_tag_img0002.cr2').touch()
        result = check_already_renamed(tmp_path)
        assert len(result) == 2

    def test_empty_directory(self, tmp_path):
        assert check_already_renamed(tmp_path) == []


# ---------------------------------------------------------------------------
# process_file
# ---------------------------------------------------------------------------

class TestProcessFile:
    _EXIF_DATE = '2023:06:15 10:30:22'

    def _make_tags(self):
        tag = MagicMock()
        tag.__str__ = lambda self: TestProcessFile._EXIF_DATE
        return {'Image DateTime': tag}

    def test_dry_run_prints_rename(self, tmp_path, capsys):
        img = tmp_path / 'IMG_0042.jpg'
        img.touch()
        with patch('shoot.exifread.process_file', return_value=self._make_tags()):
            process_file(img, tag='vacation', dry_run=True)
        out = capsys.readouterr().out
        assert '20230615_103022_vacation_img_0042.jpg' in out
        assert img.exists()  # not renamed in dry run

    def test_execute_renames_file(self, tmp_path):
        img = tmp_path / 'IMG_0042.jpg'
        img.touch()
        with patch('shoot.exifread.process_file', return_value=self._make_tags()):
            process_file(img, tag='vacation', dry_run=False)
        assert not img.exists()
        assert (tmp_path / '20230615_103022_vacation_img_0042.jpg').exists()

    def test_lowercases_extension(self, tmp_path):
        img = tmp_path / 'DSF4989.RAF'
        img.touch()
        with patch('shoot.exifread.process_file', return_value=self._make_tags()):
            process_file(img, tag='shoot', dry_run=False)
        result = list(tmp_path.glob('*.raf'))
        assert len(result) == 1
        assert result[0].name == '20230615_103022_shoot_dsf4989.raf'

    def test_lowercases_stem(self, tmp_path):
        img = tmp_path / 'DSF4989.jpg'
        img.touch()
        with patch('shoot.exifread.process_file', return_value=self._make_tags()):
            process_file(img, tag='shoot', dry_run=False)
        assert (tmp_path / '20230615_103022_shoot_dsf4989.jpg').exists()

    def test_skips_missing_exif(self, tmp_path, capsys):
        img = tmp_path / 'IMG_0042.jpg'
        img.touch()
        with patch('shoot.exifread.process_file', return_value={}):
            process_file(img, tag='vacation', dry_run=False)
        assert img.exists()  # not renamed
        assert 'Warning' in capsys.readouterr().out

    def test_skips_non_file(self, tmp_path, capsys):
        process_file(tmp_path / 'nonexistent.jpg', tag='tag', dry_run=False)
        assert 'Error' in capsys.readouterr().out

    def test_renames_xmp_sidecar(self, tmp_path):
        img = tmp_path / 'IMG_0042.jpg'
        img.touch()
        xmp = tmp_path / 'IMG_0042.xmp'
        xmp.write_text('<xmp>IMG_0042.jpg</xmp>')
        with patch('shoot.exifread.process_file', return_value=self._make_tags()):
            process_file(img, tag='vacation', dry_run=False)
        new_xmp = tmp_path / '20230615_103022_vacation_img_0042.xmp'
        assert new_xmp.exists()
        assert not xmp.exists()
        assert 'img_0042.jpg' in new_xmp.read_text()

    def test_dry_run_does_not_rename_xmp(self, tmp_path, capsys):
        img = tmp_path / 'IMG_0042.jpg'
        img.touch()
        xmp = tmp_path / 'IMG_0042.xmp'
        xmp.write_text('<xmp/>')
        with patch('shoot.exifread.process_file', return_value=self._make_tags()):
            process_file(img, tag='vacation', dry_run=True)
        assert xmp.exists()  # not touched in dry run


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_defaults(self):
        args = parse_args(['mydir', 'mytag'])
        assert args.directory == 'mydir'
        assert args.tag == 'mytag'
        assert args.execute is False
        assert args.force is False

    def test_execute_flag(self):
        args = parse_args(['mydir', 'mytag', '-x'])
        assert args.execute is True

    def test_force_flag(self):
        args = parse_args(['mydir', 'mytag', '--force'])
        assert args.force is True
