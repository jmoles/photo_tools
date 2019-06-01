import argparse
import datetime
import os
import re

import exifread

PHOTO_EXTS = ['cr2', 'jpg', 'jpeg']
IMG_RE = "^img_([0-9]{4})\.(cr2|jpg|jpeg)"
# TODO: Is this correct?
OUT_RE = "^([0-9]{8})_img_([0-9]{4})\.(cr2|jpg|jpeg)"
XMP_EXT = "xmp"

def rename_xmp(xmp_path, new_xmp_path, img_path, new_img_path):
    """ For a given old_name, rename the xmp file and instnaces 
        within XMP file to new_name.
    """

    old_img_name_only = os.path.split(img_path)[-1].strip()
    new_img_name_only = os.path.split(new_img_path)[-1].strip()

    with open(xmp_path, 'r') as f:
        filedata = f.read()

    filedata = filedata.replace(old_img_name_only, new_img_name_only)

    with open(new_xmp_path, 'w') as f:
        f.write(filedata)

    # Delete the old file if this successfully completed.
    os.remove(xmp_path)


def find_xmp(fpath):
    """ For a given full path to file, fpath, find an XMP file next to it and 
        return the name if found.
    """

    fdir, fname = os.path.split(fpath)
    file_name, ext = os.path.splitext(fname)

    xmp1 = os.path.join(fdir, "{}.{}".format(file_name, XMP_EXT.lower()))
    xmp2 = os.path.join(fdir, "{}.{}".format(file_name, XMP_EXT.upper()))

    if os.path.isfile(xmp1):
        return xmp1
    elif os.path.isfile(xmp2):
        return xmp2

    return None

def process_file(path, move_xmp=True, dry_run=True):

    # Verify the file looked at is indeed a file.
    if not os.path.isfile(path):
        print("Error! Provided path ({}) is not a file!".format(path))
        return

    # Determine the name and extension of the file itself.
    _, ext = os.path.splitext(path)
    ext = ext.lstrip(".").lower()
    curr_name = os.path.split(path)[-1].strip()
    curr_dir = os.path.split(path)[0]
    xmp_full = find_xmp(path)

    # Attempt to extract the image number from the filename.
    # This assumes filename is in the canon naming convention.
    img_re_m = re.match(IMG_RE, curr_name, flags=re.IGNORECASE)
    try:
        img_num = img_re_m.group(1)
    except IndexError:
        print("Error! No Image number found on {}!".format(curr_name))
        return
    except AttributeError:
        # Doesn't match the format....just skip it.
        return

    # Extract the EXIF information of date and convert it to a datetime.
    with open(path, 'rb') as f:
        tags = exifread.process_file(f)
    date = parse_date(str(tags["Image DateTime"]))


    # Determine the new name for the file(s).
    new_name = ("{}_img_{}".format(date.strftime("%Y%m%d"), img_num))
    new_file_path = os.path.join(os.path.split(path)[0], "{}.{}".format(new_name, ext))
    new_xmp_path = os.path.join(os.path.split(path)[0], "{}.{}".format(new_name, XMP_EXT))

    # Rename the file and and perform rename on XMP.
    if dry_run:
        print("Rename: {} -> {}".format(os.path.realpath(path), new_file_path))
        if xmp_full is not None:
            print("Rename: {} -> {}".format(os.path.realpath(xmp_full), new_xmp_path))
    else:
        os.rename(os.path.realpath(path), new_file_path)
        if xmp_full is not None:
            rename_xmp(xmp_full, new_xmp_path, path, new_file_path)


def parse_args():
    parser = argparse.ArgumentParser(
            description="Renames photo files to desired convention.")
    parser.add_argument('directory', type=str, help="Directory to work on.")
    parser.add_argument('-n', 
                        '--dry-run', 
                        help="Directory to work on.", 
                        action="store_true")

    return parser.parse_args()

def parse_date(date_str):
    """ Takes a date_str in an EXIF format and converts it to a python 
        datetime. 
    """
    return datetime.datetime.strptime(date_str, "%Y:%m:%d %H:%M:%S")

def main():
    args = parse_args()

    files = os.listdir(path=args.directory)

    for curr_file in files:
        full_path = os.path.join(args.directory, curr_file)
        if not os.path.isfile(full_path):
            continue
        root, ext = os.path.splitext(full_path)
        ext = ext.lstrip('.').lower()
        if ext in PHOTO_EXTS:
            process_file(full_path, dry_run=args.dry_run)


if __name__ == "__main__":
    main()

