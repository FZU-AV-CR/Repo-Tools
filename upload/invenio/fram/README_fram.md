# FRAM Data Pipeline

Software for processing and archiving data from the FRAM robotic telescopes
(Photometric Robotic Atmospheric Monitor) operated by FZU — Institute of Physics
of the Czech Academy of Sciences. The telescopes are located at CTA-N (La Palma),
Auger (Argentina) and CTA-S (Paranal, Chile).

The pipeline extracts metadata from FITS observation files, validates and enriches
it with image-derived quantities (FWHM, sky position, image statistics), and
publishes records to the [Physica.science](https://physica.science) repository
(InvenioRDM). Over 5 million records covering ~200 TB of data from 2007 onward
have been processed with this software (FRAMDATASET-01).

---

## Repository contents

### Upload and metadata pipeline

| File | Role |
|---|---|
| `fram_upload.py` | Direct upload to the repository. FITS discovery, header extraction, metadata building, image quality measurement (FWHM via SEP). Entry point for the upload flow. |
| `fram_make_json.py` | Bulk JSON generation. Walks a FITS tree and writes one JSON metadata file per FITS file for S3-based bulk import by the repository. Used for the main archive ingest. |
| `correct_jsons.py` | Patches generated JSON files: normalises subjects, abstract, related resources, record URL and publication date. |
| `extract_photometry.py` | Aperture photometry pipeline for science frames. Produces per-object Parquet files with calibrated magnitudes, astrometric refinement and zero-point. |
| `calibrate.py` | Per-camera CCD calibration: overscan crop, bias subtraction and non-linearity correction. Used by all tools above. |
| `bulk_async.py` | Generic async upload engine: concurrency, resume, circuit breaker, rate limiting. Shared across metadata models. |
| `async_upload.py` | Per-record HTTP pipeline: create draft → upload file → publish. |
| `adapters.py` | Registry mapping model names (`fram`, `itk`, `sipm`, `delphi`) to adapter modules. |
| `fram_upload_with_retry.py` | Wrapper that runs `fram_upload.py` in multiple passes, automatically retrying failed records. |

### FITS compression

| File | Role |
|---|---|
| `cnv_cli.py` | CLI entry point (`fram-convert`). |
| `cnv_converter.py` | `FpackConverter` and `CopyConverter` (for testing). |
| `cnv_runner.py` | Parallel conversion orchestration with atomic output and cross-device safety. |
| `cnv_paths.py` | Output path computation preserving the `data3/` archive layout. |

The compression package is also installable as `fram-fits-converter` via `pip`
(see `pyproject.toml`).

---

## Dependencies

### Upload and metadata pipeline

```
Python >= 3.10
astropy
numpy
scipy
healpy
sep          # star detection / FWHM (optional; --no-quality if not installed)
nrp_cmd      # InvenioRDM client
```

Install:
```bash
pip install astropy numpy scipy healpy sep nrp_cmd
```

### Photometry (`extract_photometry.py`)

```bash
pip install astropy numpy scipy statsmodels stdpipe
```

### FITS compression

```bash
pip install fram-fits-converter   # installs the cnv_* package
# or from source:
pip install .

# Also requires the external fpack utility (part of CFITSIO):
# https://heasarc.gsfc.nasa.gov/fitsio/
```

---

## Typical workflow

### 1. Generate JSON metadata for bulk S3 import

```bash
python3 fram_make_json.py \
    --root /mnt/data3 \
    --out ./json \
    --workers 16 \
    --publication-date 2026-11-01 \
    2>&1 | tee run.log
```

Resume an interrupted run by rerunning the same command — already-written JSON
files are skipped automatically.

### 2. Correct and patch generated JSON files

```bash
python3 correct_jsons.py \
    --source ./json/cta-n/2024 \
    --out ./json_corrected \
    --site cta-n \
    --date 2026-11-01 \
    --apply
```

Preview without writing: omit `--apply`.

### 3. Direct upload (smaller batches)

```bash
export INVENIO_TOKEN="your-token"

python3 fram_upload.py \
    --environment production \
    --data-root /mnt/data3/cta-n/2024 \
    --date-pattern "202404*" \
    --max-concurrency 4 \
    --log-file logs/upload.log \
    --stats-path logs/upload_stats.csv
```

Dry run first (no network needed):
```bash
python3 fram_upload.py --environment production --data-root /mnt/data3 --dry-run
```

### 4. Compress FITS files

```bash
fram-convert \
    --root /mnt/data3 \
    --destination /mnt/data3_compressed \
    --workers 8
```

### 5. Photometry

```bash
python3 extract_photometry.py \
    --dark /mnt/masters/masterdark.fits \
    --cat /mnt/catalogs/gaia.parquet \
    --photodir ./photometry \
    /mnt/data3/cta-n/2024/20240401/*/WF0/*.fits
```

---

## Frame rejection

Frames are excluded from processing at two levels (see `README_upload.md` for
details):

1. **Observatory level** — frames that failed astrometric solving are placed in
   `bad/` subdirectories by the site software. The pipeline skips them via
   `--exclude-dirs bad` (the default).
2. **Pipeline level** — `validate_metadata()` rejects frames with unknown
   `IMAGETYP` (`E_BAD_TYPE`), missing required fields (`E_MISSING_FIELD`),
   unreadable files (`E_UNREADABLE`), implausible dates (`E_BAD_DATE_RANGE`),
   and optionally science frames without a WCS solution (`E_NO_WCS` with
   `--require-wcs`).

Rejected files are recorded in `errors.csv` with a reason code; they are never
silently skipped.

---

## Documentation

| Document | Contents |
|---|---|
| `README_upload.md` | Full parameter reference for `fram_upload.py` and the upload engine |
| `README_make_json.md` | Full parameter reference for `fram_make_json.py` |
| `README_metadata.md` | Field-by-field reference: where every metadata value comes from, validation rules, known limitations |
| `README_converter.md` | Usage and options for the FITS compression tool |

---

## License

Apache License 2.0 — see `LICENSE`.
