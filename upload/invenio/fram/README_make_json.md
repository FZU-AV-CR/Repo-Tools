# fram_make_json.py

Walks a folder of FRAM FITS files and writes **one JSON metadata file per FITS file** into a mirrored directory tree. The JSON has the same content `fram_upload.py` builds for the Physics repository, without the upload engine. A later upload stage can use the JSON files as they are.

## Requirements

- Python 3.10+
- `astropy`, `numpy`, `healpy`, `scipy`
- `calibrate.py` in the same directory as the script (overscan cropping)

```bash
pip install astropy numpy healpy scipy
```

## Quick start

```bash
# Only two site-years, 16 parallel workers
python3 fram_make_json.py --root /mnt --out ./json \
    --include-paths data3/cta-n/2021,data3/auger2/2023 --workers 16
```

Set `publication_date` to a fixed date, or a specified number of days from
today. If neither option is used, it remains today's date:

```bash
# Use the same fixed publication date for every generated JSON
python3 fram_make_json.py --root /mnt --out ./json --publication-date 2026-11-01

# Set publication date to 30 days from the date this run starts
python3 fram_make_json.py --root /mnt --out ./json --publication-date-offset-days 30
```

Run long jobs inside `tmux` (or `screen`) and keep a log:

```bash
tmux new -s fram
python3 fram_make_json.py --root /mnt/data3 --out ./json --workers 16 2>&1 | tee -a run.log
```

## Output

For `/mnt/data3/cta-n/2021/20210409/03185/x.fits` the script writes `<out>/data3/cta-n/2021/20210409/03185/x.json`:

```json
{
  "metadata": { "...": "..." , "related_resources": ["..."] },
  "files": {
    "enabled": true,
    "entries": [{ "key": "x.fits", "path": "data3/cta-n/2021/20210409/03185/x.fits" }]
  },
  "access": { "record": "public", "files": "public", "embargo": { "active": false, "reason": null } },
  "community": "fram",
  "model": "fram"
}
```

- `access.status` is intentionally absent; the server computes it.
- JSON files are written to a temporary name and renamed, so a crash never leaves a half-written `.json`. Leftover `*.tmp<pid>` files can be deleted: `find <out> -name '*.tmp*' -delete`.
- Output names use the FITS file stem, so `x.fits` and `x.fit` in the same folder would collide (unlikely).

## Path keys (the "anchor")

Every file gets a key that starts at the **anchor** directory (default `data3`):

```
/mnt/data3/cta-n/2021/20210409/03185/x.fits  ->  data3/cta-n/2021/20210409/03185/x.fits
```

The key is used for the `files.entries[].path` value, for the mirrored output location, and for `--include-paths` / `--skip-paths` matching. If the anchor is not in the path, the key falls back to the path relative to `--root`. Change the anchor with `--anchor`.

## Options

| Option | Meaning |
|---|---|
| `--root DIR` | Folder scanned recursively for `.fits`, `.fit`, `.fts` (not needed in patch mode) |
| `--out DIR` | Output folder for the JSON tree |
| `--anchor NAME` | Directory name where keys start (default `data3`) |
| `--include-paths A,B` | Process **only** these anchor-relative subtrees, e.g. `data3/cta-n/2021,data3/auger2/2023` |
| `--include-paths-file F` | Same, one path per line (`#` comments allowed) |
| `--skip-paths A,B` | Prune these subtrees recursively (e.g. site-years already uploaded). Wins over include |
| `--skip-paths-file F` | Same, one path per line |
| `--exclude-dirs A,B` | Directory **names** skipped at any depth (default `bad`; pass `""` to disable) |
| `--date-pattern P` | Globs for `YYYYMMDD` day folders, e.g. `"202204*,202205*"` |
| `--publication-date YYYY-MM-DD` | Fixed publication date in generated JSON (mutually exclusive with offset) |
| `--publication-date-offset-days N` | Set publication date N days from today; N must be 0 or greater |
| `--site NAME` | Override site detection for every file |
| `--workers N` | Parallel worker processes (default 1) |
| `--max-files N` | Process at most N new files this run (0 = no limit) |
| `--overwrite` | Regenerate JSON files that already exist |
| `--errors-csv F` | Errors file (default `<out>/errors.csv`) |
| `--progress-interval S` | Seconds between progress lines (default 30) |
| `--site-year-resources CSV` | Site-year related resources table (see below) |
| `--site-year-resource S,Y,DOI,NAME` | Inline site-year resource, repeatable |
| `--require-site-year-resource` | Files without a matching row go to `errors.csv` |
| `--require-wcs` | Reject `object` frames without a valid astrometric solution (WCS) — writes them to `errors.csv` as `E_NO_WCS` instead of generating a JSON without sky-position fields. No effect on calibration frames. Off by default. |
| `--dry-run` | Extract and validate metadata for every FITS file but do not write any JSON. Use this to check a new data root for invalid or rejected frames before a full run. Incompatible with `--patch-related-resources`. |
| `--patch-related-resources` | Patch mode (see below) |

Paths in `--include-paths` and `--skip-paths` accept globs per path component, e.g. `data3/cta-n/202[01]`. Skipped and non-included directories are pruned during the walk, so they are never traversed.

## Large runs and resuming

- A FITS file is skipped if its output JSON already exists. Rerunning the same command resumes where the previous run stopped; only files that were in flight are redone.
- **Use the same `--out`** (and the same working directory if it is a relative path like `./json`), the same `--anchor`, and no `--overwrite`. Otherwise the existing files are not recognised and everything is redone.
- Split a big job into batches with `--include-paths`, `--date-pattern` or `--max-files`.
- Extraction reads the full pixel data (mean, median, overscan bias), so it is CPU- and I/O-heavy. A single process managed about 1.3 files/s in practice. Use `--workers` well above 1 for large trees, and keep it below your core count and available RAM (each worker holds several copies of a full frame in memory; budget roughly 1 GB per worker as a rule of thumb).
- Ctrl-C is safe. Finished JSON files stay valid.

## Site-year related resources (optional)

By default `related_resources` contains only `FRAM_FZU_root`. To add a site-year resource (DOI and name), provide a CSV and/or inline entries:

```csv
site,year,doi,name,invenio_url
auger,2023,10.83100/xxxx-xxxx,FRAM_2023_auger,
cta-n,2021,https://doi.org/10.83100/yyyy-yyyy,FRAM_2021_cta-n,
```

- `invenio_url` is read but ignored. UTF-8 with BOM and `,` / `;` / tab separators are accepted.
- `doi` can be bare (`10.83100/...`), `doi:...`, or the full URL; it is stored as `https://doi.org/...`.
- Lookup uses the **folder names** in the key: `data3/<site>/<year>/...` matches the row (site, year), case-insensitive. The `site` values must therefore equal your folder names (`auger` vs `auger2`).
- A match puts the site-year entry **before** `FRAM_FZU_root`. Inline entries override CSV rows for the same site and year.
- No matching row: the file gets `FRAM_FZU_root` only, with one warning per site-year in the log. With `--require-site-year-resource` the file goes to `errors.csv` as `no_resource` instead, and no JSON is written.
- The table is validated at startup. A bad year, DOI or name, or a duplicate site and year, stops the run and lists every problem.

## Patch mode

Updates only `metadata.related_resources` in **already generated** JSON files. No FITS file is read, so it is fast.

```bash
python3 fram_make_json.py --patch-related-resources --out ./json \
    --site-year-resources resources.csv --workers 8
```

- Needs `--out` and a resource table. No `--root`.
- `--include-paths`, `--skip-paths`, `--exclude-dirs`, `--date-pattern`, `--max-files` and `--workers` apply to the JSON tree.
- Files already correct are left untouched, so running it again is safe. All other fields stay identical.
- The resource list becomes exactly what the table says. If a row is removed and you patch again, that file goes back to `FRAM_FZU_root` only.

## errors.csv

Appended to on every run (it can contain repeated rows for the same key if a file fails again; deduplicate by `key` when reading). Columns: `timestamp, key, status, error`.

| Status | Meaning |
|---|---|
| `invalid` | Missing `site`, `ccd`, `camera_serial` or `binning`, zero usable size, unknown `IMAGETYP` (`E_BAD_TYPE`), or missing WCS on an object frame with `--require-wcs` (`E_NO_WCS`). No JSON written |
| `error` | Extraction failed (unreadable file, missing header keyword, ...). No JSON written |
| `no_resource` | `--require-site-year-resource` set and no row matched |

Files with `invalid` or `error` have no JSON, so they are retried on every rerun.

## Console log

Progress lines look like `processed=... | found=... ok=... exists=... | N files/s`. The final line starts with `Done in ...s` followed by the counters. If that line has not appeared, the run is still going or was killed.

## Known caveats

- `calibrate.py` raises a `TypeError` when `PRODUCT_ID` is missing and the image shape is not one of its known layouts. Such files end up in `errors.csv` as `error`.
- Astropy's SIP notice ("CTYPE is missing a -SIP suffix") is suppressed. It is harmless, and `fram_upload.py` builds the WCS the same way.
- The script only reads FITS files and writes to `--out`. It never modifies the source, so it is safe to run while another process (e.g. `rclone copy`) reads the same files, although both share the storage bandwidth.
- The script only creates JSON files. It does not create repository records, so it cannot produce duplicate records there.
