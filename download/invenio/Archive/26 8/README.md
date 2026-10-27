# Physics-repository download pipeline

Mirrors the upload pipeline's shape exactly: a shared engine, split the
same way (`async_*` = per-item primitives + client/env config,
`bulk_*` = orchestrator: CLI, resume, retries, circuit breaker, progress),
plus a thin adapter per metadata model.

```
Download/
  async_download.py      # per-file download, environments, client factories
  bulk_download.py        # CLI, resume/retry, circuit breaker, progress, shutdown
  download_adapters.py    # --adapter registry (fram / delphi / sipm / itk)
  FRAM/fram_download.py   # entry point: python3 fram_download.py [flags]
  DELPHI/delphi_download.py
  SIPM/sipm_download.py
  ITK/itk_download.py
```

Run a model's own entry point, never `bulk_download.py` directly (same
convention as `bulk_async.py`):

```bash
python3 fram_download.py --environment test1 --dry-run --ids 3g7r6-bx383,saejn-6wb44
python3 fram_download.py --environment test1 --filter metadata.site=LaPalma --year 2025
python3 fram_download.py --environment production --max-concurrency 4 --output-dir /mnt/data/fram
```

`--adapter` is baked in by each entry point; everything else (record
selection, environment, concurrency, retries...) is a flag on top.

## Record selection

Three ways to pick what to download, and you can combine query + filter
flags freely — everything gets ANDed together:

- `--ids id1,id2,...` or `--ids-file path.txt` — an explicit list. When
  either of these is given, all the flags below are ignored entirely.
- `--query 'target:M31'` — a free-text OpenSearch query.
- Structured metadata filters, three modes covering any field — keyword,
  range, or regex:
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
- `--year 2025` — convenience shorthand for a full-year created-date range.
- `--created-from`/`--created-to` (ISO dates, inclusive) — explicit
  created-date range. `--created-after`/`--created-before` still work as
  aliases for these.

With no selection at all, it downloads **every** record the adapter's
model matches — this uses `nrp-cmd`'s own `scan()`, which paginates and
date-bisects automatically, so it's safe to point at a whole community
even at FRAM/Delphi scale. Narrow it with the flags above for anything
short of "download the whole thing."

All field names (`--filter`/`--filter-range`/`--filter-regex`) are passed
through as-is — they must match the actual indexed field path for the
model in question, which can differ from the raw metadata key. Confirm
against a real search response (e.g. `--dry-run` and check the resulting
record count) before relying on an unfamiliar field path.

## Credentials & environment

Same convention as upload: `--token` flag → `INVENIO_TOKEN` environment
variable → interactive hidden prompt. Nothing is ever hardcoded in a
script. `--environment {local,test1,production}` selects which repository
to talk to; `production` requires typing `PRODUCTION` to confirm (or
`--yes` to skip that, e.g. in a non-interactive batch job).

## Resume, retries, and the circuit breaker

Everything is keyed by `record_id:file_key` in the stats CSV
(`<output-dir>/download_stats.csv` by default). Re-running the same
command:

- skips files that already downloaded successfully or already exist
  locally with the correct size (no CSV needed for that particular
  check — it happens before any network call).
- retries failed files up to `--max-retries` with backoff.
- if failures spike, a circuit breaker (identical design to upload's)
  pauses new downloads for a cooldown, then probes recovery — a bad
  patch of network doesn't need to fail out the whole run.

`--dry-run` still selects records and lists files (so you get an accurate
count and can sanity-check `--filter`/`--query`), it just skips the
transfers themselves.

## Big files

FRAM FITS frames and large Delphi payloads use `nrp-cmd`'s multi-part
parallel GET once a file exceeds `--multipart-threshold` (200 MiB by
default), split into `--parts` (or an explicit `--part-size`). A shared
`--transfer-weight-budget` (same idea as upload's) keeps a handful of huge
multi-part downloads from saturating the connection on top of whatever
`--max-concurrency` allows.

## Open items / please confirm before a real run

1. **`MODEL_NAME` per adapter** (in each `*_download.py`) is this
   pipeline's best guess, not a confirmed value — only Delphi's
   (`"particles"`) has a solid source (async_upload.py's own docstring).
   Confirm the rest against a live repository before relying on them:
   `nrp-cmd repository read physica-test1`, or fetch
   `https://test1.physics.du.cesnet.cz/.well-known/repository` and check
   the `models` keys.
2. **Production URL** (`https://invenio.fzu.cz/`) and `local`/`test1`
   values are copied straight from `async_upload.py`'s `ENVIRONMENTS`, so
   those should already be correct.
3. **`--transfer-weight-budget` default (50)** is copied from upload's
   restored Delphi-script value — sanity check it for download traffic
   specifically with Cesnet before a large FRAM run; download payloads per
   file are generally much bigger than upload ones.
4. **Shared per-experiment file dedup** (e.g. a repeated README) isn't
   implemented here — it wasn't in scope for this pass, and FRAM's own
   upload side (`fram_upload.py`) notes it hasn't wired one up yet either
   (`DEFAULT_README_FILE = None`, "open item"). If/when uploads start
   attaching a shared file per experiment, the download side can special
   case it (hard-link/copy from a local cache instead of re-fetching an
   identical file for every record) — flag it and this can be added.
