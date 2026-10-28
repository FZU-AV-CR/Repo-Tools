# Physics-repository download pipeline

Mirrors the upload pipeline's shape: a shared engine split the same way
(`async_*` = per-item primitives and client/environment config,
`bulk_*` = orchestrator: CLI, resume, retries, circuit breaker, progress),
plus a thin adapter per metadata model.

```
Download/
  async_download.py        # per-file download, environments, client factories
  bulk_download.py         # CLI, resume/retry, circuit breaker, progress, shutdown
  download_adapters.py     # --adapter registry (fram / delphi / sipm / itk)
  FRAM/fram_download.py    # entry point: python3 fram_download.py [flags]
  DELPHI/delphi_download.py
  SIPM/sipm_download.py
  ITK/itk_download.py
```

Run a model's own entry point, never `bulk_download.py` directly (same
convention as `bulk_async.py`):

```bash
python3 fram_download.py --environment test1 --dry-run
python3 fram_download.py --environment test1 --ids 3g7r6-bx383,saejn-6wb44
python3 fram_download.py --environment test1 --filter metadata.site=LaPalma --year 2025
python3 fram_download.py --environment production --max-concurrency 4 --output-dir /mnt/data/fram
```

`--adapter` is injected by each entry point; everything else (record
selection, environment, concurrency, retries...) is a flag on top.

## Record selection

Three ways to pick what to download; query and filter flags can be freely
combined — everything is ANDed together:

- `--ids id1,id2,...` or `--ids-file path.txt` — an explicit list. When
  either is given, all flags below are ignored.
- `--query 'target:M31'` — a free-text OpenSearch query.
- Structured metadata filters:
  - `--filter FIELD=VALUE` (repeatable) — exact keyword match, e.g.
    `--filter metadata.site=LaPalma`
  - `--filter-range FIELD=MIN:MAX` (repeatable) — numeric or date range;
    either bound may be left empty for an open range, e.g.
    `--filter-range metadata.alt_az.azimuth=100:200` or
    `--filter-range metadata.exposure=:60`
  - `--filter-regex FIELD=REGEX` (repeatable) — regex match on a keyword
    field, e.g. `--filter-regex metadata.target='M[0-9]+'`. OpenSearch/
    Lucene regex syntax — similar to but not identical to Python `re`
    (no lookaround); escape a literal `/` as `\/`.
- `--year 2025` — convenience shorthand for a full-year date range.
- `--created-from`/`--created-to` (ISO dates, inclusive) — explicit date
  range. `--created-after`/`--created-before` are aliases.
- `--date-field FIELD` — which field the three flags above filter on.
  Defaults to the adapter's `DEFAULT_DATE_FIELD` if set, otherwise the
  repository's own `created` field (record ingestion time).

  **This matters and is easy to get wrong.** The repository's `created`
  field records when a file was *ingested*, not when the observation was
  made. For FRAM, records are uploaded well after the fact, so `--year
  2021` against `created` will typically match nothing. FRAM's adapter
  therefore defaults `--date-field` to `metadata.observation_time` (the
  real exposure timestamp from the FITS `DATE-OBS` header). Other models
  (Delphi/SiPM/ITk) do not yet have a `DEFAULT_DATE_FIELD` set and fall
  back to `created` — use `--date-field` explicitly for those.

  A run using `--year`/`--created-from`/`--created-to` logs which field
  it is using, so a "zero results" outcome is not silent. If the default
  field path turns out not to be indexed as expected, try
  `--date-field metadata.dates.date` or `--date-field created`.

With no selection at all the pipeline downloads every record the adapter's
model matches, using `nrp-cmd`'s `scan()` which paginates and
date-bisects automatically — safe to point at a whole community even at
FRAM scale. Narrow with the flags above for anything short of "download
everything."

All field names passed to `--filter`/`--filter-range`/`--filter-regex`
must match the actual indexed field path for the model. Confirm against a
real search response (e.g. with `--dry-run`) before relying on an
unfamiliar path.

## Credentials and environment

Same convention as upload: `--token` flag → `INVENIO_TOKEN` environment
variable → interactive hidden prompt. Nothing is ever hardcoded in a
script. `--environment {local,test1,production}` selects which repository
to talk to; `production` requires typing `PRODUCTION` to confirm (or
`--yes` to skip, e.g. in a non-interactive batch job).

## Resume, retries, and the circuit breaker

Progress is tracked per `record_id:file_key` in a stats CSV
(`<output-dir>/download_stats.csv` by default). Re-running the same
command:

- skips files that already downloaded successfully or already exist
  locally with the correct size (checked before any network call, no CSV
  required for this).
- retries failed files up to `--max-retries` times with backoff.
- if failures spike, a circuit breaker pauses new downloads for a
  cooldown period, then probes recovery — a bad patch of network does not
  fail the whole run.

`--dry-run` still performs record and file listing (so you get an accurate
count and can sanity-check filters), but skips the transfers themselves.

## Large files

FRAM FITS frames and large Delphi payloads use `nrp-cmd`'s multi-part
parallel GET once a file exceeds `--multipart-threshold` (200 MiB by
default), split into `--parts` chunks (or an explicit `--part-size`). A
shared `--transfer-weight-budget` keeps a handful of large multi-part
downloads from saturating the connection independently of
`--max-concurrency`. Confirm a suitable budget value with Cesnet before
scaling up a large FRAM run.

## Metadata sidecar files

By default each record's metadata JSON is saved alongside the downloaded
files under `<output-dir>/_metadata/`. Pass `--no-metadata` to skip this.
