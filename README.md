# Dataset Toolkit

Tools for downloading, preparing, transforming, and uploading large scientific datasets and metadata to NRP/Invenio using async workflows.

The project focuses on:
- High-volume uploads (single and bulk) to NRP/Invenio
- Metadata transformation (CERN-like input -> Delphi-compatible schema)
- Controlled concurrency (zipping and uploads)
- Structured performance logging (JSONL/CSV)
- Analysis/visualization utilities for results and metadata

## Main Features

- Download from CERN Open Data Portal via xrootd
- FRAM and other physical data pipeline
- Async uploader for a single record
- Bulk uploader that uses the single file uploader with prioritization and concurrency limits
- Metadata filling/transformation into Delphi-style output
- Conditional zipping (large file sets), including archive repair/update logic
- Concurrency limits (weighted uploads, controlled zip workers)
- Structured performance logs for monitoring long-running jobs
- Notebook-based analysis and visualization (including Sankey charts)

## FRAM Data Pipeline

The FRAM pipeline (`upload/invenio/`) processes and archives observation data from the FRAM robotic telescopes (Photometric Robotic Atmospheric Monitor) operated by FZU Institute of Physics of the Czech Academy of Sciences. It extracts and validates metadata from FITS files, enriches it with image-derived quantities (FWHM, sky position, image statistics), and publishes records to the Physics repository.

See `upload/invenio/fram/README_fram.md` for further documentation.

## Requirements

- Python 3.11+
- `nrp_cmd` async client for communication with NRP/Invenio, only works on Linux or WSL
  - If you have problems with `nrp_cmd` installation because of aiohttp, try 
```
sudo apt update
sudo apt install build-essential python3-dev
```
- `xrootd` client for downloading from CERN Open Data Portal
- Access credentials/config for the target NRP/Invenio instance
- requirements.txt for analysis and requirements_upload.txt for upload dependencies


## Typical Workflow

0. Obtain data and metadata (e.g., from CERN Open Data Portal).
   - Expected to be too specific for every source for standardization
1. Prepare:
   - dataset directory tree `data/`
   - metadata directory (`<recid>.json`)
   - optional master cache for bulk mode
2. Run single upload or bulk upload.
3. Collect structured stats (`jsonl`/`csv`) for performance tracking.
4. Analyze outputs via notebooks in `download/` and `figs/`.

## Directory Overview

- `upload/`  
  Base upload logic (single, bulk, async) with modules for different experiments, including `upload/Delphi/` for the current Delphi workflow, `upload/samples/` for sample record uploads and `upload/invenio/` for FRAM upload and metadata extraction.

- `download/`  
  Download automation tools and post-run analysis assets/stat notebooks.

- `figs/`  
  Visualization notebooks and generated figures (e.g., Sankey charts).

- `data/`  
  Raw input datasets organized by recid (e.g., `data/<recid>/dataset1`, `data/<recid>/dataset2`, ...).

- `metadata/`  
  Source metadata files organized by recid.

- `stats/`  
  Output structured logs and outputs from download and upload runs for analysis.

## Usage (upload)

Single record:
- Input: dataset folder + metadata folder (or direct metadata file)
- Output: uploaded files + transformed metadata + stats entry
- Example: `upload/samples/sample_upload.py` demonstrates a single-record Delphi upload flow using a secure repo token from the environment or a prompt.

Bulk:
- Input: data root + metadata root + master list of records (recids)
- Behavior: uploads all recids, with configured prioritization and concurrency limits
- Output: continuous structured stats and logs for each recid

Faster bulk upload can be achieved by uploading directly to S3 and then syncing the records to Invenio but is currently being developed by Cesnet.

## Logging and Performance Stats

The uploader supports structured per-record metrics such as:
- start/end timestamps
- duration
- file count
- bytes transferred
- zip usage
- status/error

Choose JSONL or CSV based on downstream analysis needs.

## Notes

- Designed for very large-scale transfers; tune concurrency and retry limits for your environment.
- Keep credentials and endpoint configuration outside version control.
