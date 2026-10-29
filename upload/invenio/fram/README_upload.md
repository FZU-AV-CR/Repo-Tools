# FRAM Upload Pipeline

Uploads FRAM telescope FITS files to the Physics repository (InvenioRDM,
via `nrp_cmd`). Built on a shared, model-agnostic engine also used by
other metadata models (ITk, SiPM, Delphi); FRAM's specifics live in one
adapter file.

## Files

| File | Role |
|---|---|
| `fram_upload.py` | **Entry point.** Run this one directly. Injects FRAM's defaults and hands off to `bulk_async.py`. FITS discovery, header extraction, metadata building all live here. |
| `bulk_async.py` | Generic orchestration engine: concurrency, resume, circuit breaker, rate limiting, graceful shutdown. Shared across all metadata models — do not run directly (it will tell you so). |
| `async_upload.py` | Generic per-record pipeline: extract → validate → create → upload file → publish, plus the transfer/rate limiters. Shared across all models. |
| `adapters.py` | Registry mapping a model name (`fram`, `itk`, `sipm`, `delphi`) to its adapter module. |
| `calibrate.py` | FITS overscan-cropping/bias-subtraction used when computing FRAM metadata (not part of the uploaded file itself). |
| `TEST_async_upload.py` | Standalone, self-contained single-file upload test — no imports from the files above. For quickly checking connectivity/schema/community/model handling against one FITS file before running the real pipeline. |

## Quick start

```bash
export INVENIO_TOKEN="your-token"

python3 fram_upload.py \
  --environment test1 \
  --data-root "/mnt/data3/cta-n/2024" \
  --date-pattern "202404*" \
  --max-concurrency 4 \
  --log-file logs/upload.log \
  --stats-path logs/upload_stats.csv \
  --disable-schema
```

Dry run first if you haven't validated a given data-root recently — no
network/token needed, just checks metadata extraction:
```bash
python3 fram_upload.py --environment test1 --data-root "..." --dry-run
```

---

## Parameter reference

### Required

| Flag | Meaning |
|---|---|
| `--data-root PATH` | Root directory to recursively walk for FITS files. |
| `--environment {local,test1,production}` | Which repository to upload to. Default `local`. |

`--metadata-dir` is also technically required by the underlying engine but is **not meaningful for FRAM** (FRAM has no separate metadata files — each FITS header is its own metadata). `fram_upload.py` fills this in automatically to match `--data-root`; you never need to set it yourself.

### Authentication

| Flag | Meaning |
|---|---|
| `--token TOKEN` | API token. **Prefer the `INVENIO_TOKEN` environment variable** — a token on the command line leaks into shell history and process listings. Use `--token` only when that's genuinely more convenient (e.g. inside a loop script that already handles the token itself). |
| `--yes` | Skip the interactive `production` confirmation prompt. Needed for unattended/scripted production runs. |

### FRAM-specific flags

These flags are **not** part of the generic engine's `--help` output — they're parsed directly out of the command line by `fram_upload.py` itself and won't show up if you run `python3 fram_upload.py --help` (that only lists the generic engine's flags).

| Flag | Meaning |
|---|---|
| `--date-pattern "202404*"` | Comma-separated glob pattern(s) matched against `YYYYMMDD` day-folder names, wherever they occur under `--data-root`. Applies across every site simultaneously (`cta-n`, `auger2`, ...) since it matches the folder name alone, not the full path. Omit entirely to upload every date. Example: `--date-pattern "202204*,202205*"` for April+May 2022. |
| `--exclude-dirs "bad"` | Comma-separated glob pattern(s) matched against **any** directory name at **any** depth — matching folders are skipped entirely. **Defaults to `"bad"` even if you never pass this flag.** Pass `--exclude-dirs ""` explicitly to disable exclusion and upload everything, including `bad/` folders. |
| `--require-wcs` | Reject `object`-type frames that have no valid astrometric solution (WCS) in their header, writing them to `errors.csv` as `E_NO_WCS` instead of uploading them without sky-position fields. Has no effect on calibration frames (`dark`, `flat`, `bias`, `masterdark`, `masterflat`, `dcurrent`) — those never carry a WCS. Off by default: without this flag, science frames without a WCS are uploaded normally, just without `center`, `radius`, `footprint` and related fields. |
| `--preset-identifier` | Pre-fill the record's `identifiers` field with a URL constructed as `base-url/site-filename` before the record is created. Intended for **bulk/Cesnet import flows** where the landing-page URL can be determined in advance. **Off by default**: without this flag, `identifiers` is left empty and Invenio assigns its own record ID after creation — this is the correct behaviour for direct uploads via this pipeline. |

#### How bad frames are excluded — the two-stage quality gate

FRAM observatories follow a convention of moving frames that failed astrometric solving into a subdirectory named `bad/` (done by the observatory control software or operators, outside this pipeline). The pipeline's default `--exclude-dirs bad` then silently prunes every such folder during the directory walk — the files are never read, extracted, or counted. This is the primary mechanism for excluding frames with hardware problems or poor observing conditions (clouds, tracking failures), since these are exactly the cases that cause the astrometric solver to fail.

The result is a two-stage quality gate:

1. **Observatory level** — frames without a valid astrometric solution are placed in `bad/` by the site software or operators.
2. **Pipeline level** — `--exclude-dirs bad` (the default) skips all `bad/` folders during the walk.

`--require-wcs` adds an optional third stage for cases where `bad/` sorting has not been done or is incomplete: it rejects any `object` frame that reaches the pipeline but still lacks a WCS, writing it to `errors.csv` rather than uploading it silently without sky-position metadata. For bulk historical uploads where you want to ingest everything available, leave this flag off. For ongoing ingestion of new frames, enabling it provides an additional safety net.

### Concurrency & transfer

| Flag | Default | Meaning |
|---|---|---|
| `--max-concurrency N` | `4` | How many records' extract→create→upload→publish pipelines run concurrently. **Largely capped in practice by `--rate-limit`, below** — raising this past ~10-20 usually buys little once the rate limiter is the real bottleneck. |
| `--transfer-weight-budget N` | `50` | Shared budget across all in-flight file transfers. Small (\<10MB) transfers cost 1 unit; large/multipart transfers cost 5. Independent of `--max-concurrency` — this is what actually bounds simultaneous bytes-in-flight. |
| `--file-concurrency N` | `1` | How many files of a *single record* upload at once. Irrelevant for FRAM (always exactly one file per record); relevant for adapters like Delphi with several files per record. |

### Request-rate limiting

| Flag | Default | Meaning |
|---|---|---|
| `--rate-limit "N/seconds"` | `450/60` **and** `4500/3600` | Global cap on real HTTP requests/second, enforced across every concurrent task. Repeat the flag for multiple simultaneous windows — **all supplied windows apply together (AND, not OR)**, matching how Cesnet's own server-side limiter is configured (`500/min` **and** `5000/hour`). The defaults sit ~10% under Cesnet's reported numbers as a safety margin. For a long-running bulk job, the *longer* window is usually the real bottleneck (the hourly cap works out to roughly 1.25 req/s sustained — well under what the per-minute cap alone would allow) — **confirm with Cesnet whether this can be raised or scoped to this specific job** before assuming `--max-concurrency` is what's limiting your throughput. |

Every real HTTP call (create draft, upload file, fill identifier, publish) draws from this same shared budget, regardless of how many concurrent tasks are running.

### Schema / model / community

| Flag | Meaning |
|---|---|
| `--schema-url URL` | Explicit `$schema` value to send. Defaults to `fram_upload.py`'s `DEFAULT_SCHEMA_URL` (`local://fram-v1.0.0.json`) if omitted — **confirmed correct for `local` only so far.** |
| `--disable-schema` | Omit `"$schema"` from the record body entirely instead of sending a possibly-wrong value. `--schema-url` is ignored when this is set. Cesnet's own `nrp_cmd` usage example creates records with just `metadata` + `model=`, no `$schema` at all. **Recommended for test1/production until the correct schema value is confirmed.** |

`model="fram"` and the `fram` community slug are set automatically inside `fram_upload.py`'s metadata — no flag needed. **Not yet independently confirmed correct** against test1/production; if record creation fails even with `--disable-schema`, this is the next thing to check with Cesnet.

### Circuit breaker

Protects a long run from a systemic failure (bad token, repo outage, wrong schema) by pausing new uploads once too many recent attempts have failed — see `CircuitBreaker`'s docstring in `bulk_async.py` for the full state machine (closed → open → half-open → closed/reopen).

| Flag | Default | Meaning |
|---|---|---|
| `--disable-breaker` | off | Disable entirely — every item is attempted regardless of failure history. Useful for isolating whether the breaker itself is responsible for a run looking "stuck" or for items being skipped, at the cost of no automatic pause if something is systemically broken. **Not recommended for an unattended bulk run.** |
| `--breaker-window-seconds` | `60` | How far back the breaker looks when judging the recent failure rate. |
| `--breaker-min-samples` | `20` | Minimum number of outcomes needed in the window before the breaker will evaluate at all — avoids tripping on a handful of early failures. |
| `--breaker-failure-threshold` | `0.5` | Failure rate (0–1) within the window that trips the breaker once `--breaker-min-samples` is met. |
| `--breaker-cooldown-seconds` | `60` | How long new uploads are blocked after tripping, before probing recovery. |
| `--breaker-half-open-probes` | `5` | How many test uploads are let through after cooldown to check if the problem cleared. |
| `--breaker-half-open-success-threshold` | `0.6` | Fraction of those probes that must succeed for the breaker to fully close again. |

**Tuning note:** the breaker only records one sample per *item* (after all its retries are exhausted), not per attempt. If you significantly raise `--max-retries`/`--retry-delay`, widen `--breaker-window-seconds` to match, or the breaker may struggle to ever accumulate enough samples to react to a real problem.

### Retries

| Flag | Default | Meaning |
|---|---|---|
| `--max-retries N` | `3` | Retry attempts per item before giving up. |
| `--retry-delay N` | `2` | Base delay in seconds between retries; scales with attempt number. |

### Validation & dry-run

| Flag | Meaning |
|---|---|
| `--dry-run` | Extract & validate metadata only — no network calls, no token needed. Always run this first against a new `--data-root`. |
| `--no-validate` | Skip the minimal required-field check (`site`, `ccd`, `camera_serial`, `binning`, usable dimensions) before upload. |

### Logs & stats output

| Flag | Default | Meaning |
|---|---|---|
| `--stats-path PATH` | `<cwd>/logs/<run-id>/upload_stats.csv` | Per-item CSV log (also used for resume — see below). An explicit path always overrides the default folder logic. |
| `--log-file PATH` | `<cwd>/logs/<run-id>/upload.log` | Rotating log file, in addition to console output. |
| `--run-id NAME` | fresh timestamp each run | Fixes the default output folder name (`<cwd>/logs/<run-id>/`) so **restarting reuses the same stats/log files** — needed for resume to work without explicitly setting `--stats-path`/`--log-file` yourself. Without it, every run gets a fresh timestamped folder and starts from scratch. |
| `--disable-stats` | off | Don't write a stats CSV at all. No resume, no end-of-run failure summary. **Testing only** — you'll re-upload everything on every run. |
| `--disable-logs` | off | Console output only, no log file. **Testing only.** |
| `--progress-interval N` | `30` | Seconds between progress log lines. |

### Resume behavior

Reruns with the **same** `--stats-path` (or `--run-id`) automatically skip
items already marked `ok`. Items with a `started` row but no terminal
status (crash mid-upload) are retried and flagged as `interrupted` at
startup — check the repository for possible orphaned drafts if that
matters to you. Retried items produce multiple rows sharing the same
`key`; the CSV has `attempt`/`max_attempts` columns specifically so you
can tell "failed once, then succeeded" apart from "truly failed" — filter
for `status=="failed"` **and** `attempt==max_attempts` to find genuine
failures, or read the end-of-run log summary, which does this for you
automatically.

---

## Common recipes

**First look at a new data-root (no network):**
```bash
python3 fram_upload.py --environment test1 --data-root "..." --dry-run
```

**Small, closely-watched test1 run, breaker off to see the true failure count:**
```bash
python3 fram_upload.py --environment test1 --data-root "..." \
  --max-concurrency 4 --disable-breaker --disable-schema \
  --log-file logs/test.log --stats-path logs/test_stats.csv
```

**One month, one site:**
```bash
python3 fram_upload.py --environment test1 --data-root "/mnt/data3/cta-n/2024" \
  --date-pattern "202404*"
```

**One month, all sites:**
```bash
python3 fram_upload.py --environment test1 --data-root "/mnt/data3" \
  --date-pattern "202404*"
```

**A run you intend to be able to restart/resume:**
```bash
python3 fram_upload.py --environment test1 --data-root "..." --run-id april2024
```

**Quick manual test, no leftover files between runs:**
```bash
python3 fram_upload.py --environment local --data-root "..." \
  --disable-stats --disable-logs
```

---

## Open items / things not yet confirmed

- **`model="fram"`** and the schema handling are not independently confirmed against test1/production — `--disable-schema` sidesteps the schema question but not the model name.
- **`--rate-limit` defaults** are Cesnet's reported numbers with a margin, not a guaranteed-safe value — confirm directly with Cesnet before a full bulk run, especially whether the limit can be raised or scoped to this job.
- `camera_serial` is read from the `PRODUCT_ID` header keyword (HIERARCH, image HDU), the same keyword `calibrate.py`'s `find_calibration_config()` uses. If `PRODUCT_ID` is absent from the header object that `crop_overscans()` receives, the latter can raise — worth checking against a real file if that path is exercised.
