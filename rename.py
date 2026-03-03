# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "exifread",
# ]
# ///

import argparse
import datetime
from pathlib import Path

import exifread

PHOTO_EXTS = {
    'cr2', 'cr3',   # Canon RAW
    'nef',           # Nikon RAW
    'arw',           # Sony RAW
    'raf',           # Fujifilm RAW
    'orf',           # Olympus RAW
    'rw2',           # Panasonic RAW
    'dng',           # Adobe DNG
    'jpg', 'jpeg',   # JPEG
}

XMP_EXT = 'xmp'


def parse_date(date_str: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")


def find_xmp(path: Path) -> Path | None:
    for candidate in [path.with_suffix('.xmp'), path.with_suffix('.XMP')]:
        if candidate.is_file():
            return candidate
    return None


def rename_xmp(xmp_path: Path, new_xmp_path: Path, old_img_name: str, new_img_name: str) -> None:
    content = xmp_path.read_text()
    content = content.replace(old_img_name, new_img_name)
    new_xmp_path.write_text(content)
    xmp_path.unlink()


def process_file(path: Path, tag: str, dry_run: bool = True) -> None:
    if not path.is_file():
        print(f"Error: {path} is not a file.")
        return

    ext = path.suffix.lstrip('.').lower()
    original_stem = path.stem.lower().strip('_')
    xmp_path = find_xmp(path)

    with path.open('rb') as f:
        tags = exifread.process_file(f, stop_tag='Image DateTime')

    if 'Image DateTime' not in tags:
        print(f"Warning: No EXIF date found in {path.name}, skipping.")
        return

    dt = parse_date(str(tags['Image DateTime']))
    new_stem = f"{dt.strftime('%Y%m%d')}_{dt.strftime('%H%M%S')}_{tag}_{original_stem}"
    new_path = path.with_name(f"{new_stem}.{ext}")
    new_xmp_path = path.with_name(f"{new_stem}.{XMP_EXT}")

    if dry_run:
        print(f"Rename: {path.name} -> {new_path.name}")
        if xmp_path:
            print(f"Rename: {xmp_path.name} -> {new_xmp_path.name}")
    else:
        path.rename(new_path)
        if xmp_path:
            rename_xmp(xmp_path, new_xmp_path, path.name, new_path.name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Renames photo files to a date/time/tag convention."
    )
    parser.add_argument('directory', type=str, help="Directory to process.")
    parser.add_argument('tag', type=str, help="Tag to embed in filenames (e.g. 'liam06mo').")
    parser.add_argument('-x', '--execute', action='store_true',
                        help="Actually rename files (default is a dry-run preview).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    directory = Path(args.directory)
    tag = args.tag.lower()
    dry_run = not args.execute

    if dry_run:
        print("Dry run — no files will be changed. Pass -x to apply.\n")

    for path in sorted(directory.iterdir()):
        if path.is_file() and path.suffix.lstrip('.').lower() in PHOTO_EXTS:
            process_file(path, tag=tag, dry_run=dry_run)


if __name__ == '__main__':
    main()
