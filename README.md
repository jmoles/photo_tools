# photo_tools

A script for renaming photo files using EXIF metadata.

## Output format

Files are renamed to the following convention, all lowercase:

```
{date}_{time}_{tag}_{original}.{ext}
```

| Part | Example | Description |
|------|---------|-------------|
| `date` | `20260118` | EXIF capture date (YYYYMMDD) |
| `time` | `152258` | EXIF capture time (HHMMSS) |
| `tag` | `liam06mo` | User-supplied label for the shoot |
| `original` | `dsf4768` | Original filename stem, lowercased |

Example: `DSF4768.RAF` → `20260118_152258_liam06mo_dsf4768.raf`

XMP sidecar files are renamed to match and their internal filename references are updated automatically.

## Supported formats

| Format | Cameras |
|--------|---------|
| CR2, CR3 | Canon RAW |
| NEF | Nikon RAW |
| ARW | Sony RAW |
| RAF | Fujifilm RAW |
| ORF | Olympus RAW |
| RW2 | Panasonic RAW |
| DNG | Adobe DNG |
| JPG, JPEG | JPEG |

## Requirements

- [uv](https://docs.astral.sh/uv/) — no other setup needed.

## Usage

```sh
# Preview renames (dry run, default)
uv run rename.py /path/to/photos liam06mo

# Apply renames
uv run rename.py /path/to/photos liam06mo -x
```

```
usage: rename.py [-h] [-x] directory tag

Renames photo files to a date/time/tag convention.

positional arguments:
  directory    Directory to process.
  tag          Tag to embed in filenames (e.g. 'liam06mo').

options:
  -x, --execute  Actually rename files (default is a dry-run preview).
  -h, --help     Show this help message and exit.
```
