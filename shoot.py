# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "exifread",
# ]
# ///

import argparse
import datetime
import re
import sys
from pathlib import Path

import exifread

from photo import ALREADY_RENAMED_RE, SHOOT_PHOTO_EXTS as PHOTO_EXTS, parse_exif_dt, rename_file


def process_file(path: Path, tag: str, dry_run: bool = True) -> None:
    if not path.is_file():
        print(f"Error: {path} is not a file.")
        return

    with path.open('rb') as f:
        tags = exifread.process_file(f, stop_tag='Image DateTime')

    if 'Image DateTime' not in tags:
        print(f"Warning: No EXIF date found in {path.name}, skipping.")
        return

    dt = parse_exif_dt(str(tags['Image DateTime']))
    if dt is None:
        print(f"Warning: Unable to parse EXIF date in {path.name}, skipping.")
        return

    rename_file(path, dt, tag, dry_run=dry_run)


def check_already_renamed(directory: Path) -> list[str]:
    """Return names of any photo files that look like they've already been renamed."""
    return [
        p.name for p in directory.iterdir()
        if p.is_file()
        and p.suffix.lstrip('.').lower() in PHOTO_EXTS
        and ALREADY_RENAMED_RE.match(p.stem)
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Renames photo files to a date/time/tag convention."
    )
    parser.add_argument('directory', type=str, help="Directory to process.")
    parser.add_argument('tag', type=str, help="Tag to embed in filenames (e.g. 'liam06mo').")
    parser.add_argument('-x', '--execute', action='store_true',
                        help="Actually rename files (default is a dry-run preview).")
    parser.add_argument('--force', action='store_true',
                        help="Bypass the already-renamed check.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    directory = Path(args.directory)
    tag = args.tag.lower()
    dry_run = not args.execute

    if not args.force:
        already_renamed = check_already_renamed(directory)
        if already_renamed:
            print("Warning: some files in this directory look like they've already been renamed:")
            for name in sorted(already_renamed):
                print(f"  {name}")
            print()
            if not dry_run:
                print("Aborting. Re-run with --force if you really want to proceed.")
                sys.exit(1)
            print("Dry run continuing — pass --force -x to execute anyway.\n")

    if dry_run:
        print("Dry run — no files will be changed. Pass -x to apply.\n")

    for path in sorted(directory.iterdir()):
        if path.is_file() and path.suffix.lstrip('.').lower() in PHOTO_EXTS:
            process_file(path, tag=tag, dry_run=dry_run)


if __name__ == '__main__':
    main()
