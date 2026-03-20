# photo-tools

A Python toolkit for renaming and consolidating photo libraries using EXIF metadata.

## Scripts

| Script | Purpose |
|--------|---------|
| `rename.py` | Targeted rename of a single shoot directory by EXIF date |
| `consolidate.py` | Bulk library consolidation — dedup, organise, sidecar handling |

## Running

```bash
# Rename a single shoot (dry-run by default)
uv run rename.py /path/to/shoot tagname
uv run rename.py /path/to/shoot tagname -x   # apply

# Consolidate library (dry-run by default)
uv run consolidate.py
uv run consolidate.py --execute              # apply
uv run consolidate.py --source /path/to/unorg --dest /path/to/library  # override config
```

## Configuration

Copy `config.example.toml` to `config.toml` and fill in your paths. `config.toml` is
gitignored. CLI arguments always override config file values.

## Tests

```bash
uv run --group dev pytest tests/             # all tests
uv run --group dev pytest tests/test_unit.py         # unit tests only (no exiftool needed)
uv run --group dev pytest tests/test_integration.py  # integration tests (requires exiftool)
```

Integration tests require `exiftool` to be installed. Tests that depend on real camera
sample files skip automatically if the samples are not present (they are stored via Git LFS —
run `git lfs pull` to fetch them).

## Dependencies

- Python 3.11+
- `exiftool` system binary (install via package manager)
- `uv` for running scripts and managing dev dependencies
- No third-party Python packages required at runtime (stdlib only)

## Supported formats

| Category | Extensions |
|----------|-----------|
| Photos | CR2, CR3, RAF, DNG, HEIC, JPG, JPEG, TIF, TIFF, PNG, WEBP, BMP |
| Video | MOV, MP4, M4V, MPG, MPEG, AVI, WMV |
| Sidecars | XMP, PP3 (both travel with their paired original) |

## consolidate.py — output layout

```
<dest>/
  YYYY/MM/          ← organised photos
  _undated/         ← files where only mtime was available (flagged _mtime in name)
  dupes/YYYY/MM/    ← hash duplicates (never deleted)
  movies/YYYY/MM/   ← standalone video files
  _review/          ← PSDs, Lightroom catalogs, unpaired thumbnails
```

## Date extraction fallback chain

1. Embedded EXIF (`DateTimeOriginal` → `CreateDate` → `ModifyDate`)
2. Paired XMP sidecar EXIF
3. Date regex parsed from filename — `_fndate` appended to output filename
4. Filesystem mtime — `_mtime` appended, file goes to `_undated/`

## Resume / cache

`consolidate.py` maintains a SQLite cache (`consolidate_cache.db` by default). Re-running
after an interruption skips already-processed files using a fast size+mtime fingerprint
check — no re-hashing of completed files.
