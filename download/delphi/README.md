# DELPHI Download

This folder contains tools for downloading DELPHI datasets from CERN Open Data, caching record metadata, and exporting file inventories for analysis. The main downloader is designed for large runs: it uses parallel workers, retry logic, resume checks, and checksum/size verification to make long downloads more reliable.

## What This Downloader Can Do

- Query DELPHI records from CERN Open Data using `experiment:DELPHI`
- Build and maintain a master cache in `delphi_records_master.json`
- Download files in parallel with configurable workers and retry behavior
- Skip already valid files (resume-safe behavior)
- Verify downloaded files against expected size and checksum (Adler-32)
- Write run statistics and error logs for later analysis

## Download target

- The downloader automatically builds a file list from the CERN API, but since it can only list 10k datasets you can also supply a custom file
- To build this file we used the Delphi records from https://github.com/cernopendata/opendata.cern.ch/tree/master/data/records and `build_master_cache.py`

## Files in This Folder

- `main.py` - Parallel DELPHI downloader and verifier
- `list_delphi_files.py` - Builds a CSV file list from JSON record lists in `lists/`
- `delphi_files.csv` - Example/generated CSV file inventory
- `delphi_records_master.json` - Master cache of records and file states
- `build_master_cache.py` - Script to build the master cache from the list of records
- `stats.ipynb` - Jupyter notebook for analyzing download stats
- `errors.ipynb` - Jupyter notebook for analyzing error logs and failures

## Requirements

- Python 3.8+
- `requests`
- `cernopendata-client`

Install basic dependencies from the project root:

```powershell
pip install -r requirements.txt
```

If `cernopendata-client` is not already installed in your environment, install it separately:

```powershell
pip install cernopendata-client
```

## Quick Start

Run from the project root (`C:\Users\Lenovo\PycharmProjects\test`):

```powershell
python .\download\delphi\main.py --workers 12 --protocol http
```

Useful options:

- `--max-recid 10` - limit number of records for a small test run
- `--output-dir <path>` - choose where `data/`, `metadata/`, and `stats/` are written
- `--retry-limit <n>` and `--retry-sleep <s>` - tune retry behavior
- `--verify-recid <id>` - verify one already-downloaded record and exit
- `--protocol <http|xrootd>` - choose download protocol, xrootd can be faster but may require additional setup
- `--workers <n>` - number of parallel download workers, adjust based on your network and CPU

Example test run:

```powershell
python .\download\delphi\main.py --max-recid 5 --workers 4 --output-dir .
```

Verify one record:

```powershell
python .\download\delphi\main.py --verify-recid 81001 --output-dir .
```

## Outputs

By default, output is written under the project root (`../..` relative to `main.py`):

- `data/<recid>/...` - downloaded files
- `metadata/<recid>.json` - cached metadata per record
- `stats/download_stats.csv` - per-file transfer stats
- `stats/errorsYYYYMMDD_HHMMSS.log` - timestamped error log
- `download/delphi/delphi_records_master.json` - master cache with progress flags

## CSV Export Helpers

Create a CSV from list files in `download/delphi/lists/`:

```powershell
python .\download\delphi\list_delphi_files.py
```

Create a CSV from the master cache:

```powershell
python .\download\delphi\list_files.py
```

## Troubleshooting

- If downloads fail intermittently, increase `--retry-limit` and `--retry-sleep`.
- If the record search appears incomplete, the CERN API can enforce result caps for very large queries.
- If files are not skipped on rerun, check that output points to the same `--output-dir` and existing `data/` tree.
- If checksum verification fails repeatedly, remove the affected file and rerun to force a clean download.

