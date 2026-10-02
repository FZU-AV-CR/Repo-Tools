"""
Generic bulk async orchestrator for Physics-repository uploads.

This is the shared engine, not a standalone script -- it's driven by a
model-specific entry point (itk_upload.py, sipm_upload.py, fram_upload.py,
delphi_upload.py, ...), which fills in that model's default paths and
calls bulk_async.main(). Running this file directly
(`python3 bulk_async.py`) raises an error pointing to the correct entry
point instead.

Mirrors fram_bulk_async.py (the original, model-specific FRAM script this
was generalized from) as closely as possible: resume via a stats CSV,
bounded concurrency, graceful shutdown on SIGINT/SIGTERM, periodic
progress logging, and a --dry-run mode that skips repository client setup
entirely (no network/token needed).

Differences from fram_bulk_async.py:
  - Work items are discovered by <adapter>.discover_items(), which for the
    current TXT/ZIP-based adapters (ITk, SiPM) takes --metadata-dir/
    --data-root instead of a single --input-dir. A future FRAM adapter
    doing a recursive filesystem walk for FITS files is free to interpret
    those same two flags however makes sense for it (e.g. --data-root as
    the FITS tree root and --metadata-dir unused).
  - The circuit breaker is a rate/cooldown design (see CircuitBreaker's
    docstring below), not fram_bulk_async.py's original "N consecutive
    failures, trips permanently" version -- that version's notion of
    "consecutive" stopped being meaningful once concurrency got high
    enough that dozens of uploads are in flight and completing out of
    order, and a permanent trip meant a transient blip could kill the
    rest of a multi-day run.
  - Everything else (resume/dedup keyed off "key" in the stats CSV,
    progress tracker, retry wrapper, shutdown handling) is unchanged in
    shape from fram_bulk_async.py.

The adapter itself is no longer hardcoded here -- it's resolved at
runtime by adapters.py, from --adapter (this file's own CLI flag) or the
PHYSICS_ADAPTER environment variable. Each model's entry-point script sets
one of these for you, so `python3 sipm_upload.py ...` and
`python3 itk_upload.py ...` both work without editing this file. To add a
new metadata model, see adapters.py -- no changes needed here.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import signal
import time
from collections import deque
from pathlib import Path

import adapters
import async_upload
from async_upload import WeightedSemaphore, RateLimiter

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"ok", "failed", "skipped_invalid", "dryrun"}


# ============================================================
# RESUME / INTERRUPTED-RUN DETECTION
# (same logic as fram_bulk_async.py's _scan_stats -- generic since it's
# already keyed off the "key" column, not anything FITS-specific)
# ============================================================


def _scan_stats(stats_path: "Path | None") -> tuple[set[str], set[str]]:
    """Return (uploaded_keys, interrupted_keys).

    uploaded_keys: keys with a terminal status == 'ok' -- safe to skip on
    resume.
    interrupted_keys: keys with a 'started' row but no terminal row at all.
    These may have created an orphaned draft record in the repository if
    the previous run crashed between record creation and the final stats
    write. They are re-attempted on resume (this script does not
    deduplicate against the repository itself), but are surfaced as a
    startup warning so they can be checked manually if duplicates are a
    concern.

    stats_path=None (--disable-stats) means no persistent record exists at
    all -- resume/dedup is impossible, so this always returns empty sets.
    """
    if stats_path is None or not stats_path.exists():
        return set(), set()

    started: set[str] = set()
    terminal: set[str] = set()
    uploaded: set[str] = set()

    try:
        with stats_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                key = row.get("key")
                status = str(row.get("status", "")).strip().lower()
                if not key:
                    continue
                if status == "started":
                    started.add(key)
                elif status in TERMINAL_STATUSES:
                    terminal.add(key)
                    if status == "ok":
                        uploaded.add(key)
    except Exception as exc:
        logger.warning("Failed to parse stats file %s: %s", stats_path, exc)
        return set(), set()

    interrupted = started - terminal
    return uploaded, interrupted


def _final_rows_per_key(stats_path: "Path | None") -> dict[str, dict]:
    """Return the LAST non-'started' stats row seen for each key, as a
    dict of key -> row dict.

    This is the row that determines a key's actual final outcome. With
    retries, a key can have multiple terminal rows (e.g. attempt=1
    status=failed, attempt=2 status=failed, attempt=3 status=ok) --
    scanning the raw CSV for status=="failed" directly would wrongly flag
    that key even though it ultimately succeeded. Since CSV rows are
    appended in chronological order, the last row seen for a key IS its
    final outcome (across this run and any prior resumed runs), so a
    plain dict keyed by `key` naturally ends up holding exactly that.
    """
    if stats_path is None or not stats_path.exists():
        return {}
    final: dict[str, dict] = {}
    try:
        with stats_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                key = row.get("key")
                status = str(row.get("status", "")).strip().lower()
                if not key or status == "started":
                    continue
                final[key] = row
    except Exception as exc:
        logger.warning("Failed to parse stats file %s: %s", stats_path, exc)
        return {}
    return final


def _estimate_requests_for_row(row: dict) -> int:
    """Conservative estimate of how many real HTTP requests a single stats
    CSV row's attempt consumed, used only for seeding the rate limiter's
    history on process startup (see RateLimiter.seed_from_history() and
    _reconstruct_rate_limiter_history() below). Deliberately errs toward
    OVERestimating rather than underestimating: this value only controls
    how cautiously a resumed/restarted process starts out, and
    underestimating risks recreating the exact retry storm this
    reconstruction exists to prevent, while overestimating only costs a
    somewhat more conservative startup pace.

    "started" rows are not counted here at all (return 0) -- they exist to
    detect crashes (see _scan_stats()'s "interrupted" keys), and every
    attempt that reached a real terminal status also has its own terminal
    row; counting both would double-count the same underlying attempt.
    A "started" row with no matching terminal row (a genuinely interrupted
    attempt) is undercounted by this simplification -- accepted as a minor
    gap for what's already an approximation, and such rows are already
    surfaced separately as a startup warning elsewhere.
    """
    status = str(row.get("status", "")).strip().lower()
    if status == "started":
        return 0
    requests = 1  # records.create()
    try:
        bytes_uploaded = int(float(row.get("bytes_uploaded") or 0))
    except (TypeError, ValueError):
        bytes_uploaded = 0
    if bytes_uploaded > 0:
        # Mirrors async_upload.py's own multipart-burst weighting exactly,
        # so a large historical file's real request footprint is reflected
        # here the same way it would be if that upload were happening now.
        requests += async_upload._expected_multipart_parts(bytes_uploaded)
    if status == "ok":
        requests += 1  # records.publish()
    return requests


def _reconstruct_rate_limiter_history(stats_path: "Path | None", rate_limiter: "RateLimiter | None") -> tuple[int, int]:
    """Seed rate_limiter's sliding-window history from stats_path's past
    attempts, so a freshly started process (after a crash, a manual
    restart, or a new pass launched by fram_upload_with_retry.py) doesn't
    forget how much of the server's real, shared rate-limit budget was
    already consumed recently -- the server's own window doesn't reset
    just because our client did. See RateLimiter.seed_from_history()'s
    docstring for the full rationale.

    Returns (rows_scanned, total_weight_seeded), both 0 if there's nothing
    to seed from (no stats file yet, no rate limiter configured, or
    nothing recent enough to matter).
    """
    if stats_path is None or rate_limiter is None or not stats_path.exists():
        return 0, 0

    max_window = max(window for _, window in rate_limiter.limits)
    now = time.time()
    cutoff = now - max_window

    events: list[tuple[float, int]] = []
    rows_scanned = 0
    try:
        with stats_path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                start_ts_raw = row.get("start_ts")
                if not start_ts_raw:
                    continue
                try:
                    start_ts = float(start_ts_raw)
                except (TypeError, ValueError):
                    continue
                if start_ts < cutoff:
                    continue
                weight = _estimate_requests_for_row(row)
                if weight <= 0:
                    continue
                events.append((start_ts, weight))
                rows_scanned += 1
    except Exception as exc:
        logger.warning("Failed to parse stats file %s for rate-limit history reconstruction: %s", stats_path, exc)
        return 0, 0

    seeded = rate_limiter.seed_from_history(events)
    return rows_scanned, seeded


def summarize_final_outcomes(stats_path: Path) -> dict[str, list[str]]:
    """Bucket every key's true final outcome (see _final_rows_per_key) by
    status, e.g. {"ok": [...], "failed": [...], "skipped_breaker": [...]}.
    Prefer this over ad-hoc CSV scans when you need "which items actually
    failed" -- including from a notebook (upload_stats.ipynb): read the
    CSV, sort by start_ts, and keep each key's last row rather than
    filtering status=="failed" on the raw rows.
    """
    buckets: dict[str, list[str]] = {}
    for key, row in _final_rows_per_key(stats_path).items():
        status = str(row.get("status", "")).strip().lower() or "unknown"
        buckets.setdefault(status, []).append(key)
    return buckets


# ============================================================
# CIRCUIT BREAKER
# (redesigned from the "N consecutive failures" version that was
# unchanged from fram_bulk_async.py -- the original Delphi script had no
# breaker at all, but for a 100 TB/multi-day run some form of "stop
# digging" mechanism is still worth having; "consecutive" just isn't a
# meaningful signal once dozens/hundreds of uploads run concurrently.
# With --max-concurrency 100, a handful of unrelated, unlucky failures
# scattered among 100 in-flight tasks can land as "15 in a row" from this
# breaker's point of view even while the other 85 are succeeding fine.
#
# This version instead tracks the failure RATE over a recent sliding time
# window, and only trips once both a minimum sample size and a minimum
# failure rate are seen within that window -- one bad-luck cluster among
# a mostly-healthy run no longer trips it, but a genuine systemic problem
# (most uploads failing) still does, usually faster than waiting for N
# consecutive failures to accumulate.
#
# It also no longer trips permanently for the rest of the run: after a
# cooldown period it moves to a HALF_OPEN state and lets a small number
# of "probe" uploads through. If most of those succeed, it closes again
# and normal uploads resume; if they keep failing, it reopens for another
# cooldown. A transient problem (a brief network blip, a momentary server
# hiccup) no longer permanently kills the rest of a multi-day run.
# ============================================================


class CircuitBreaker:
    """Rate-based circuit breaker with a cooldown/half-open recovery cycle.

    States:
      CLOSED    -- normal operation. Every attempt's outcome (success/
                   failure) is recorded with a timestamp in a sliding
                   window of the last `window_seconds`. Once at least
                   `min_samples` outcomes are in that window, if the
                   failure rate among them is >= `failure_threshold`, the
                   breaker OPENs.
      OPEN      -- new uploads are blocked. After `cooldown_seconds` have
                   elapsed since opening, the breaker moves to HALF_OPEN.
      HALF_OPEN -- up to `half_open_max_probes` uploads are allowed
                   through as a test, one at a time (allow_start() won't
                   hand out more than that many before results come
                   back). Once that many probe results are in: if the
                   success rate among them is >= `half_open_success_
                   threshold`, the breaker CLOSES (resets its window and
                   resumes normal operation); otherwise it reOPENs for
                   another cooldown.

    All state transitions are asyncio.Lock-protected since allow_start()/
    record_result() are called concurrently from many in-flight uploads.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        window_seconds: float = 60.0,
        min_samples: int = 20,
        failure_threshold: float = 0.5,
        cooldown_seconds: float = 60.0,
        half_open_max_probes: int = 5,
        half_open_success_threshold: float = 0.6,
    ):
        self.window_seconds = window_seconds
        self.min_samples = min_samples
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.half_open_max_probes = half_open_max_probes
        self.half_open_success_threshold = half_open_success_threshold

        self._state = self.CLOSED
        self._events: deque[tuple[float, bool]] = deque()  # (monotonic ts, was_success)
        self._opened_at: float | None = None
        self._half_open_results: list[bool] = []
        self._half_open_in_flight = 0
        self._lock = asyncio.Lock()

    @property
    def tripped(self) -> bool:
        """Fast, lock-free check for logging/reporting purposes only.
        allow_start() below is the authoritative, lock-protected check
        that also drives the OPEN -> HALF_OPEN transition -- always call
        allow_start() before actually starting an upload, don't gate on
        this property directly."""
        return self._state == self.OPEN

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    async def allow_start(self) -> bool:
        """Call before starting a new upload attempt. Returns True if it
        should proceed, False if it should be skipped right now."""
        async with self._lock:
            now = time.monotonic()

            if self._state == self.OPEN:
                if self._opened_at is not None and (now - self._opened_at) >= self.cooldown_seconds:
                    self._state = self.HALF_OPEN
                    self._half_open_results = []
                    self._half_open_in_flight = 0
                    logger.warning(
                        "Circuit breaker cooldown elapsed (%.0fs) -- entering half-open state, "
                        "allowing up to %s probe upload(s) to test recovery.",
                        self.cooldown_seconds, self.half_open_max_probes,
                    )
                else:
                    return False

            if self._state == self.HALF_OPEN:
                if self._half_open_in_flight >= self.half_open_max_probes:
                    return False
                self._half_open_in_flight += 1
                return True

            return True  # CLOSED

    async def record_result(self, success: bool) -> None:
        """Call once an upload attempt (started via allow_start()
        returning True) has finished, with its outcome."""
        async with self._lock:
            now = time.monotonic()

            if self._state == self.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._half_open_results.append(success)
                if len(self._half_open_results) >= self.half_open_max_probes:
                    successes = sum(self._half_open_results)
                    total = len(self._half_open_results)
                    rate = successes / total
                    if rate >= self.half_open_success_threshold:
                        logger.warning(
                            "Circuit breaker recovery confirmed (%s/%s probes succeeded) -- "
                            "closing, resuming normal uploads.",
                            successes, total,
                        )
                        self._state = self.CLOSED
                        self._events.clear()
                    else:
                        logger.error(
                            "Circuit breaker recovery probes still failing (%s/%s succeeded) -- "
                            "reopening for another %.0fs cooldown.",
                            successes, total, self.cooldown_seconds,
                        )
                        self._state = self.OPEN
                        self._opened_at = now
                return

            self._events.append((now, success))
            self._prune(now)

            if self._state == self.CLOSED and len(self._events) >= self.min_samples:
                failures = sum(1 for _, ok in self._events if not ok)
                rate = failures / len(self._events)
                if rate >= self.failure_threshold:
                    logger.error(
                        "Circuit breaker tripped: %s/%s uploads failed in the last %.0fs "
                        "(>= %.0f%% failure-rate threshold). Pausing new uploads for %.0fs "
                        "before probing recovery. Investigate connectivity/auth/schema.",
                        failures, len(self._events), self.window_seconds,
                        self.failure_threshold * 100, self.cooldown_seconds,
                    )
                    self._state = self.OPEN
                    self._opened_at = now


# ============================================================
# PROGRESS TRACKING  (unchanged from fram_bulk_async.py)
# ============================================================


def _format_eta(seconds: float) -> str:
    if seconds != seconds or seconds == float("inf"):  # NaN or inf
        return "unknown"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m{s:02d}s"


class ProgressTracker:
    def __init__(self, total: int, interval: float):
        self.total = total
        self.interval = interval
        self.done = 0
        self.ok = 0
        self.failed = 0
        self.skipped = 0
        self._last_log = time.monotonic()
        self._start = time.monotonic()

    def record(self, kind: str) -> None:
        self.done += 1
        if kind == "ok":
            self.ok += 1
        elif kind == "failed":
            self.failed += 1
        else:
            self.skipped += 1

        now = time.monotonic()
        if now - self._last_log >= self.interval or self.done == self.total:
            elapsed = now - self._start
            rate = self.done / elapsed if elapsed > 0 else 0.0
            remaining = self.total - self.done
            eta_s = remaining / rate if rate > 0 else float("inf")
            logger.info(
                "Progress: %s/%s done (ok=%s failed=%s skipped=%s) | %.2f items/s | ETA %s",
                self.done, self.total, self.ok, self.failed, self.skipped, rate, _format_eta(eta_s),
            )
            self._last_log = now


# ============================================================
# GRACEFUL SHUTDOWN  (unchanged from fram_bulk_async.py)
# ============================================================


async def _write_skip_stats(stats_path: "Path | None", stats_format: str, key: str, reason: str) -> None:
    """Write a visible stats row for an item that was never attempted
    because the circuit breaker had tripped or shutdown was requested.

    Previously these items were only logger.info()'d and otherwise
    vanished -- with a high --max-concurrency, a burst of early failures
    can trip the breaker within seconds, after which most of a run's
    items were skipped with no trace in upload_stats.csv, making a "most
    files skipped" run look identical to a "most files failed" one from
    the stats file alone. status here is deliberately NOT one of
    bulk_async.TERMINAL_STATUSES, so these items are still picked up and
    retried (not treated as resolved) on the next run.
    """
    await async_upload._write_stats(
        stats_path,
        stats_format,
        async_upload._stats_payload(key, status=f"skipped_{reason}", error=reason, attempt=0, max_attempts=0),
    )


def _request_shutdown(stop_event: asyncio.Event, sig) -> None:
    if not stop_event.is_set():
        logger.warning(
            "Received signal %s -- will not start new uploads; waiting for "
            "in-flight uploads to finish. This may take a while under high "
            "concurrency; use a process manager to force-kill if needed.",
            getattr(sig, "name", sig),
        )
        stop_event.set()


# ============================================================
# PER-ITEM RETRY WRAPPER  (unchanged shape from fram_bulk_async.py)
# ============================================================


async def _upload_with_retries(
    client,
    item,
    stats_path: "Path | None",
    stats_format: str,
    schema_url: str,
    dry_run: bool,
    validate: bool,
    stop_event: asyncio.Event,
    retries: int = 3,
    delay: int = 2,
    upload_limiter: WeightedSemaphore | None = None,
    rate_limiter: RateLimiter | None = None,
    file_concurrency: int = 1,
    disable_schema: bool = False,
):
    retries = max(1, retries)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        if stop_event.is_set():
            logger.info("[%s] Shutdown requested, aborting before attempt %s", item.key, attempt)
            return None
        try:
            return await async_upload.upload_record_async(
                client=client,
                item=item,
                stats_path=stats_path,
                stats_format=stats_format,
                dry_run=dry_run,
                validate=validate,
                schema_url=schema_url,
                upload_limiter=upload_limiter,
                rate_limiter=rate_limiter,
                file_concurrency=file_concurrency,
                disable_schema=disable_schema,
                attempt=attempt,
                max_attempts=retries,
            )
        except Exception as exc:
            last_exc = exc
            logger.warning("[%s] Upload failed (attempt %s/%s): %s", item.key, attempt, retries, exc)
            if attempt == retries:
                raise
            await asyncio.sleep(delay * attempt)
    raise last_exc  # pragma: no cover -- loop always returns or raises above


# ============================================================
# MAIN
# ============================================================


async def main_async(args: argparse.Namespace) -> None:
    # Resolve default output locations before anything else logs or writes
    # anything, since setup_logging() (in _main_async_inner) needs the
    # final log_file path.
    #
    # Default (neither --stats-path nor --log-file given): write both into
    # a fresh cwd/logs/<run_id>/ folder, so repeated runs don't clobber
    # each other's logs/stats and don't need manual cleanup between test
    # runs. --run-id pins this to a stable, reusable folder name (e.g. for
    # a run you intend to resume/restart against the SAME stats file);
    # without it, a fresh timestamp is used every run, which means
    # resume/dedup will NOT work by default across separate invocations --
    # pass --stats-path explicitly (or use --run-id) if you need that.
    #
    # --disable-stats / --disable-logs independently suppress the stats
    # CSV / log file entirely (console output only, no resume capability)
    # -- meant for quick manual testing, where a leftover upload_stats.csv
    # you have to keep deleting between runs gets in the way. An explicit
    # --stats-path/--log-file always wins over these defaults regardless.
    run_id = args.run_id or time.strftime("%Y%m%d_%H%M%S")
    default_run_dir = Path.cwd() / "logs" / run_id

    if args.disable_stats:
        stats_path = None
    elif args.stats_path:
        stats_path = Path(args.stats_path)
    else:
        stats_path = default_run_dir / "upload_stats.csv"

    if args.disable_logs:
        log_file = None
    elif args.log_file:
        log_file = Path(args.log_file)
    else:
        log_file = default_run_dir / "upload.log"

    # The actual work happens in _main_async_inner. Wrapping it here (rather
    # than folding this try/finally into that function directly) means the
    # run id / stats path / log file get announced again as the LAST thing
    # logged no matter how the run ends: normal completion, an early
    # validation failure (return), a graceful Ctrl-C drain (the stop_event
    # mechanism further down doesn't raise, so it would already reach this
    # point naturally), or a raw KeyboardInterrupt/exception from somewhere
    # that isn't covered by that graceful path at all (e.g. interrupting an
    # interactive token prompt, or discover_items() taking a long time) --
    # this finally still fires regardless. Without this, checking only the
    # tail of a long log after an interrupt could leave you unable to find
    # which --run-id to reuse to resume, since it was otherwise only ever
    # logged once, near the very top of the file.
    try:
        await _main_async_inner(args, run_id, stats_path, log_file)
    finally:
        logger.warning(
            "Run id: %s | Stats CSV: %s | Log file: %s -- reuse this --run-id "
            "(or --stats-path) to resume this run later, regardless of how it just ended.",
            run_id, stats_path, log_file,
        )


async def _main_async_inner(
    args: argparse.Namespace, run_id: str, stats_path: "Path | None", log_file: "Path | None"
) -> None:
    async_upload.setup_logging(log_file=log_file)

    if args.disable_stats:
        logger.warning(
            "Stats CSV DISABLED (--disable-stats) -- no upload_stats.csv will be written, "
            "resume/dedup is impossible, and the end-of-run failure summary will be skipped. "
            "Intended for quick manual testing only -- do not use this for a real bulk run."
        )
    else:
        logger.info("Stats CSV: %s", stats_path)
    if args.disable_logs:
        logger.warning("Log file DISABLED (--disable-logs) -- console output only.")
    else:
        logger.info("Log file: %s", log_file)

    # Resolve + configure the metadata-model adapter (--adapter flag ->
    # PHYSICS_ADAPTER env var -> error). async_upload.configure_adapter()
    # sets its own module-level `adapter` global (used by
    # upload_record_async()) and returns the same module here for local use.
    adapter = async_upload.configure_adapter(args.adapter)
    schema_url = args.schema_url or adapter.DEFAULT_SCHEMA_URL
    # Schema is DISABLED by default (see --enable-schema's help text for
    # why) -- args.disable_schema is accepted but has no effect anymore,
    # kept only for backward compatibility with existing command lines.
    effective_disable_schema = not args.enable_schema

    if args.token:
        logger.warning(
            "Using --token from the command line; prefer the %s environment "
            "variable to avoid leaking credentials via process listings or "
            "shell history.",
            async_upload.TOKEN_ENV_VAR,
        )

    metadata_dir = Path(args.metadata_dir)
    data_root = Path(args.data_root)
    readme_file = Path(args.readme_file) if args.readme_file else None

    if not metadata_dir.exists():
        logger.error("Metadata directory does not exist: %s", metadata_dir)
        return
    if not data_root.exists():
        logger.error("Data directory does not exist: %s", data_root)
        return

    # Preflight: validate we can build a client / have a token *before*
    # doing any directory discovery. Skipped entirely for --dry-run, which
    # never touches the repository.
    client = None
    if not args.dry_run:
        try:
            client = await async_upload.create_client_for_environment(
                args.environment, token=args.token, confirm_production=not args.yes,
            )
        except Exception as exc:
            logger.error("Preflight failed -- could not create repository client: %s", exc)
            return
    else:
        logger.info("Dry-run mode: no records will be created, uploaded, or published. Skipping repository client setup.")

    items = adapter.discover_items(metadata_dir=metadata_dir, data_root=data_root, readme_file=readme_file)
    logger.info("Work items found under %s: %s", metadata_dir, len(items))
    if not items:
        logger.info("Nothing to do.")
        return

    uploaded_keys, interrupted_keys = _scan_stats(stats_path)
    if uploaded_keys:
        before = len(items)
        items = [it for it in items if it.key not in uploaded_keys]
        logger.info("Resume: skipped %s already uploaded items from %s", before - len(items), stats_path)

    if interrupted_keys:
        sample = ", ".join(list(interrupted_keys)[:20])
        logger.warning(
            "%s item(s) have a 'started' stats row from a previous run with no "
            "terminal status -- these may have created orphaned draft records "
            "in the repository if that run crashed mid-upload. They will be "
            "re-attempted now; check the repository for duplicates if this is "
            "a concern. Examples: %s%s",
            len(interrupted_keys), sample, " ..." if len(interrupted_keys) > 20 else "",
        )

    sem = asyncio.Semaphore(args.max_concurrency)
    # Bounds total in-flight *transfer weight* (large/multipart uploads
    # count for more than small/direct ones -- see async_upload.py's
    # MULTIPART_WEIGHT), independent of --max-concurrency, which only
    # bounds how many records' extract/create/publish steps overlap. This
    # is what actually caps how much is hitting the repository/network at
    # once. Restored from the original Delphi script, which used a fixed
    # budget of 50; exposed here as a flag since FRAM's file sizes and
    # target concurrency differ. Size this against what the target
    # environment (test1/production) can actually sustain -- confirm with
    # Cesnet rather than guessing, especially before scaling up
    # --max-concurrency for the full 100 TB run.
    upload_limiter = WeightedSemaphore(args.transfer_weight_budget)

    # Global requests-per-second limiter -- see async_upload.py's
    # RateLimiter docstring. Defaults mirror Cesnet's actual reported
    # test1 config (500/min, 5000/hour) with a ~10% safety margin, since
    # the limiter is very likely shared with other traffic hitting the
    # same server and running right up against the exact configured
    # numbers leaves no room for that. Each --rate-limit "N/seconds" is
    # enforced simultaneously with every other one supplied (AND, not OR)
    # -- pass the flag multiple times for multiple windows, matching
    # flask-limiter's own semantics.
    if args.disable_rate_limit:
        rate_limiter = None
        rate_limit_specs = "disabled"
        logger.warning(
            "Rate limiting explicitly DISABLED (--disable-rate-limit) -- every real HTTP request "
            "(create, upload, publish) fires with no client-side pacing at all. Only use this once "
            "you've confirmed test1/production genuinely has no active server-side rate limiter -- "
            "if one is later re-enabled without you knowing, this flag being left on will reproduce "
            "the exact 429/retry-storm behavior this limiter exists to prevent."
        )
    else:
        rate_limit_specs = args.rate_limit if args.rate_limit is not None else ["450/60", "4500/3600"]
        rate_limits: list[tuple[int, float]] = []
        for spec in rate_limit_specs:
            try:
                count_str, window_str = spec.split("/", 1)
                rate_limits.append((int(count_str), float(window_str)))
            except (ValueError, AttributeError):
                logger.error(
                    "Invalid --rate-limit value %r -- expected 'N/seconds', e.g. '450/60'. Ignoring it.",
                    spec,
                )
        rate_limiter = RateLimiter(rate_limits) if rate_limits else None
        if rate_limiter is None:
            logger.warning(
                "No valid --rate-limit values parsed -- request-rate limiting is DISABLED. "
                "Given test1's server-side limiter (500/min, 5000/hour per Cesnet), running "
                "without this is likely to hit 429s, especially at higher --max-concurrency. "
                "If this is intentional, use --disable-rate-limit instead for a clearer signal."
            )

    # History reconstruction only makes sense when a real limiter exists --
    # whether it's absent because of --disable-rate-limit or because no
    # valid --rate-limit values were parsed makes no difference here.
    if rate_limiter is not None:
        if args.disable_rate_limit_history:
            logger.warning(
                "Rate-limit history reconstruction DISABLED (--disable-rate-limit-history) -- "
                "this process's rate limiter starts believing the FULL budget is available, even "
                "if a previous process (a prior pass, or a crashed/restarted run) already used up "
                "some of the server's real, shared window recently. Risks a burst of 429s right "
                "after startup if that's the case."
            )
        else:
            rows_scanned, seeded_weight = _reconstruct_rate_limiter_history(stats_path, rate_limiter)
            if seeded_weight > 0:
                logger.info(
                    "Rate-limit history: reconstructed ~%s request(s) worth of recent activity from "
                    "%s row(s) in %s (within the last %.0fs) -- rate limiter starting with reduced "
                    "budget to reflect what the server likely still remembers, not a fresh full quota.",
                    seeded_weight, rows_scanned, stats_path, max(window for _, window in rate_limiter.limits),
                )

    breaker = None
    if not args.disable_breaker:
        breaker = CircuitBreaker(
            window_seconds=args.breaker_window_seconds,
            min_samples=args.breaker_min_samples,
            failure_threshold=args.breaker_failure_threshold,
            cooldown_seconds=args.breaker_cooldown_seconds,
            half_open_max_probes=args.breaker_half_open_probes,
            half_open_success_threshold=args.breaker_half_open_success_threshold,
        )
    else:
        logger.warning(
            "Circuit breaker DISABLED (--disable-breaker) -- every item will be attempted "
            "regardless of how many prior uploads failed. Useful for isolating whether errors "
            "are breaker-related, but means a systemic problem will not stop the run early."
        )

    logger.info(
        "Items to upload: %s (adapter=%s, environment=%s, max_concurrency=%s, "
        "transfer_weight_budget=%s, file_concurrency=%s, rate_limits=%s, "
        "schema=%s, breaker=%s, dry_run=%s)",
        len(items), adapter.__name__, args.environment, args.max_concurrency,
        args.transfer_weight_budget, args.file_concurrency, rate_limit_specs,
        "DISABLED (default -- pass --enable-schema to send $schema)" if effective_disable_schema else schema_url,
        "disabled" if breaker is None else (
            f"[window={args.breaker_window_seconds}s min_samples={args.breaker_min_samples} "
            f"failure_threshold={args.breaker_failure_threshold * 100:.0f}% "
            f"cooldown={args.breaker_cooldown_seconds}s half_open_probes={args.breaker_half_open_probes}]"
        ),
        args.dry_run,
    )

    progress = ProgressTracker(total=len(items), interval=args.progress_interval)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown, stop_event, sig)
        except NotImplementedError:
            # add_signal_handler is not available on some platforms
            pass

    start = time.perf_counter()

    async def _run_one(item):
        async with sem:
            breaker_blocked = breaker is not None and not await breaker.allow_start()
            if stop_event.is_set() or breaker_blocked:
                reason = "shutdown" if stop_event.is_set() else "breaker"
                logger.info("[%s] Skipping (shutdown requested or circuit breaker open)", item.key)
                progress.record("skipped")
                if not args.dry_run:
                    await _write_skip_stats(stats_path, "csv", item.key, reason)
                return None
            try:
                result = await _upload_with_retries(
                    client=client,
                    item=item,
                    stats_path=stats_path,
                    stats_format="csv",
                    schema_url=schema_url,
                    dry_run=args.dry_run,
                    validate=not args.no_validate,
                    stop_event=stop_event,
                    retries=args.max_retries,
                    delay=args.retry_delay,
                    upload_limiter=upload_limiter,
                    rate_limiter=rate_limiter,
                    file_concurrency=args.file_concurrency,
                    disable_schema=effective_disable_schema,
                )
                if breaker is not None:
                    await breaker.record_result(success=True)
                progress.record("ok" if result is not None else "skipped")
                return result
            except Exception as exc:
                if breaker is not None:
                    await breaker.record_result(success=False)
                progress.record("failed")
                return exc

    tasks = [asyncio.create_task(_run_one(it)) for it in items]
    await asyncio.gather(*tasks, return_exceptions=True)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.remove_signal_handler(sig)
        except NotImplementedError:
            pass

    logger.info(
        "%s finished in %.2fs. total=%s ok=%s failed=%s skipped=%s%s",
        "Dry run" if args.dry_run else "Bulk upload",
        time.perf_counter() - start,
        progress.done, progress.ok, progress.failed, progress.skipped,
        f" (see {stats_path} for full per-item status)" if stats_path is not None else " (--disable-stats was set, no per-item record was kept)",
    )

    if not args.dry_run and stats_path is not None:
        # progress.failed/progress.ok above count *attempts*, not items --
        # a key retried twice before succeeding counts as 1 failed + 1 ok
        # there. This resolves each key to its true final outcome (see
        # summarize_final_outcomes()'s docstring) for an unambiguous
        # "what actually needs attention" report.
        outcomes = summarize_final_outcomes(stats_path)
        failed_keys = outcomes.get("failed", [])
        if failed_keys:
            sample = ", ".join(failed_keys[:20])
            logger.error(
                "%s item(s) FAILED after exhausting all retries (see %s, status=='failed' rows with "
                "attempt==max_attempts, for full error details). Examples: %s%s",
                len(failed_keys), stats_path, sample, " ..." if len(failed_keys) > 20 else "",
            )
        skipped_breaker = outcomes.get("skipped_breaker", [])
        skipped_shutdown = outcomes.get("skipped_shutdown", [])
        if skipped_breaker:
            logger.warning(
                "%s item(s) were never attempted because the circuit breaker was open -- "
                "not failures, will be retried on the next run.",
                len(skipped_breaker),
            )
        if skipped_shutdown:
            logger.warning(
                "%s item(s) were never attempted due to shutdown -- not failures, will be "
                "retried on the next run.",
                len(skipped_shutdown),
            )
        if not failed_keys and not skipped_breaker and not skipped_shutdown:
            logger.info("No failed or breaker/shutdown-skipped items in %s.", stats_path)
    elif not args.dry_run and stats_path is None:
        logger.warning(
            "Stats CSV was disabled (--disable-stats) -- no failure summary is available. "
            "progress totals above (ok=%s failed=%s skipped=%s) count attempts, not resolved "
            "items, and any per-item error detail was only ever sent to the log/console.",
            progress.ok, progress.failed, progress.skipped,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk async upload of Physics-repository records from TXT metadata files + matching data files. "
                     "The metadata model is selected via --adapter (or the PHYSICS_ADAPTER env var)."
    )
    parser.add_argument(
        "--adapter", choices=adapters.available(), default=None,
        help=f"Which metadata-model adapter to use. Falls back to the {adapters.ENV_VAR} "
             "environment variable if omitted (each model's own entry-point script, e.g. "
             "sipm_upload.py, sets this for you).",
    )
    parser.add_argument("--metadata-dir", required=True, help="Directory containing the *.txt metadata files (one per record)")
    parser.add_argument("--data-root", required=True, help="Directory containing the matching data files (ZIPs, or a per-record folder of ZIPs, depending on the adapter)")
    parser.add_argument(
        "--readme-file", default=None,
        help="Optional shared README.txt uploaded alongside every record's data. Omit to upload without a README.",
    )
    parser.add_argument(
        "--stats-path",
        default=None,
        help="Path to the stats CSV (also used for resume). Defaults to "
             "<cwd>/logs/<run-id>/upload_stats.csv if omitted -- see --run-id and --disable-stats.",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="Stable name for this run's default output folder (<cwd>/logs/<run-id>/), used only "
             "when --stats-path/--log-file are omitted. Without it, a fresh timestamp is used every "
             "run, so restarting without --run-id or an explicit --stats-path will NOT resume from a "
             "previous run's progress -- pass a fixed --run-id (or --stats-path) for a run you intend "
             "to restart/resume.",
    )
    parser.add_argument(
        "--disable-stats", action="store_true",
        help="Do not write a stats CSV at all -- no resume/dedup, no end-of-run failure summary. "
             "For quick manual testing only; do not use for a real bulk run.",
    )
    parser.add_argument(
        "--disable-logs", action="store_true",
        help="Do not write a log file -- console output only. For quick manual testing only.",
    )
    parser.add_argument("--max-concurrency", type=int, default=4, help="Maximum concurrent uploads")
    parser.add_argument(
        "--transfer-weight-budget", type=int, default=50,
        help="Shared transfer budget across all in-flight file uploads. Small/direct ('L') transfers "
             "consume 1 unit; large/multipart ('M', >= 10MB) transfers consume 5 (see async_upload.py's "
             "MULTIPART_WEIGHT). This is independent of --max-concurrency and is what actually bounds "
             "load on the repository/network -- raising --max-concurrency alone does not increase how "
             "many bytes are in flight at once. Restored from the original Delphi script's fixed budget "
             "of 50; confirm a suitable value against the target environment (test1/production) with "
             "Cesnet before scaling up, especially for large bulk FRAM runs.",
    )
    parser.add_argument(
        "--file-concurrency", type=int, default=1,
        help="How many of a single record's own files upload concurrently (each still also goes "
             "through --transfer-weight-budget). Irrelevant for adapters with one file per record "
             "(e.g. FRAM); relevant for adapters with several files per record (e.g. Delphi).",
    )
    parser.add_argument(
        "--disable-rate-limit", action="store_true",
        help="Disable client-side request-rate limiting entirely (--rate-limit and its defaults are "
             "both ignored). Only use this once you've confirmed the target environment genuinely has "
             "no active server-side rate limiter -- if one exists (or is re-enabled later) without you "
             "knowing, running with this flag reproduces the exact 429/retry-storm behavior the rate "
             "limiter exists to prevent. Confirming via response headers is unreliable: a server can "
             "enforce a limit while never sending headers about it either way -- ask the repository "
             "operator directly if you're not certain.",
    )
    parser.add_argument(
        "--rate-limit", action="append", default=None,
        help="Global request-rate limit as 'N/seconds', e.g. '450/60' = at most 450 requests in any "
             "60s window. Repeat the flag for multiple simultaneous windows (all are enforced together, "
             "AND not OR) -- the default mirrors Cesnet's reported test1 server-side limiter config "
             "(500/min, 5000/hour) with a ~10%% safety margin: '--rate-limit 450/60 --rate-limit "
             "4500/3600'. For a long-running bulk job the longer window is usually the real bottleneck "
             "(4500/3600 = ~1.25 req/s sustained, well under the 450/60 = ~7.5 req/s the per-minute "
             "window alone would allow) -- confirm with Cesnet whether this can be raised or scoped to "
             "this job specifically before assuming --max-concurrency is what limits your throughput. "
             "Pass an empty value or omit entirely with caution -- see the warning logged at startup "
             "if no rate limit ends up configured.",
    )
    parser.add_argument(
        "--disable-rate-limit-history", action="store_true",
        help="On startup, this process normally scans --stats-path for recent activity (within the "
             "largest configured --rate-limit window) and seeds the rate limiter with an estimate of "
             "what it already consumed -- since restarting the client (a crash, a manual resume, or "
             "each pass of fram_upload_with_retry.py launching a fresh process) does not reset the "
             "SERVER's own window; it only forgets our own bookkeeping. Without this reconstruction, "
             "a resumed run believes the full rate-limit budget is available immediately, which can "
             "recreate the exact 429 burst the limiter exists to prevent. Pass this flag to disable "
             "that reconstruction and start with a fresh, full budget regardless of recent history "
             "(e.g. if --stats-path points at an old, unrelated file).",
    )
    parser.add_argument(
        "--environment", choices=sorted(async_upload.ENVIRONMENTS), default="local",
        help="Which repository to upload to (local / test1 / production)",
    )
    parser.add_argument(
        "--schema-url", default=None,
        help="Invenio $schema value; defaults to the selected adapter's own DEFAULT_SCHEMA_URL if omitted. "
             "Confirmed only for local so far -- verify before using with test1/production. Ignored "
             "entirely unless --enable-schema is set (schema is DISABLED by default -- see --enable-schema).",
    )
    parser.add_argument(
        "--disable-schema", action="store_true",
        help="No longer does anything -- omitting \"$schema\" is now the DEFAULT. Kept only so an "
             "existing command line that already includes this flag keeps working unchanged. Use "
             "--enable-schema to opt back into sending \"$schema\".",
    )
    parser.add_argument(
        "--enable-schema", action="store_true",
        help="Send \"$schema\" in the record payload (the old default behavior), instead of omitting "
             "it. Confirmed unnecessary for FRAM/test1: an actual published record resolved the correct "
             "\"$schema\" server-side purely from model=, even with no \"$schema\" sent at all. Only "
             "use this if a different adapter/environment turns out to actually need an explicit value.",
    )
    parser.add_argument(
        "--token", default=None,
        help=f"API token; prefer the {async_upload.TOKEN_ENV_VAR} environment variable over this flag",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip the interactive confirmation prompt required for --environment production",
    )
    parser.add_argument("--dry-run", action="store_true", help="Extract & validate metadata but do not create/upload/publish any records")
    parser.add_argument("--no-validate", action="store_true", help="Disable the minimal required-field check before upload")
    parser.add_argument(
        "--disable-breaker", action="store_true",
        help="Disable the circuit breaker entirely -- every item is attempted regardless of how many "
             "prior uploads failed (no rate-based tripping, no cooldown/half-open skipping). Useful for "
             "isolating whether errors you're seeing are caused by the breaker itself vs. something "
             "else (e.g. transport/rate-limit issues) -- NOT recommended for a real bulk run, since a "
             "systemic problem will then run to completion (or exhaust retries on every item) instead "
             "of pausing early.",
    )
    parser.add_argument(
        "--breaker-window-seconds", type=float, default=60.0,
        help="Circuit breaker: size of the sliding time window (seconds) over which the failure rate "
             "is evaluated. See CircuitBreaker's docstring for the full state-machine explanation.",
    )
    parser.add_argument(
        "--breaker-min-samples", type=int, default=20,
        help="Circuit breaker: minimum number of outcomes required within the window before the "
             "failure rate is evaluated at all -- prevents tripping on a tiny early sample. Scale this "
             "up with --max-concurrency (e.g. roughly a fifth to a third of it) so a normal amount of "
             "concurrent noise doesn't read as a trend before enough data has accumulated.",
    )
    parser.add_argument(
        "--breaker-failure-threshold", type=float, default=0.5,
        help="Circuit breaker: failure rate (0.0-1.0) within the window that trips the breaker once "
             "--breaker-min-samples is met, e.g. 0.5 = trips once half of recent uploads are failing.",
    )
    parser.add_argument(
        "--breaker-cooldown-seconds", type=float, default=60.0,
        help="Circuit breaker: how long to block new uploads after tripping before moving to "
             "half-open and probing recovery.",
    )
    parser.add_argument(
        "--breaker-half-open-probes", type=int, default=5,
        help="Circuit breaker: number of probe uploads allowed through in the half-open state after "
             "a cooldown, to test whether the underlying problem has cleared.",
    )
    parser.add_argument(
        "--breaker-half-open-success-threshold", type=float, default=0.6,
        help="Circuit breaker: fraction (0.0-1.0) of half-open probes that must succeed for the "
             "breaker to close and resume normal uploads; otherwise it reopens for another cooldown.",
    )
    parser.add_argument("--max-retries", type=int, default=3, help="Retry attempts per item")
    parser.add_argument("--retry-delay", type=int, default=2, help="Base delay (seconds) between retries; scales with attempt number")
    parser.add_argument("--log-file", default=None, help="Optional rotating log file path, in addition to console output")
    parser.add_argument("--progress-interval", type=float, default=30.0, help="Seconds between progress log lines")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(
        "bulk_async.py is not meant to be run directly -- it's the shared "
        "engine, driven by a model-specific entry point. Run one of:\n"
        "    python3 itk_upload.py [flags]\n"
        "    python3 sipm_upload.py [flags]\n"
        "    python3 fram_upload.py [flags]\n"
        "    python3 delphi_upload.py [flags]\n"
        "(each imports this module and calls bulk_async.main() after filling "
        "in that model's default paths and --adapter name.)"
    )


### bulk_async.py is not run directly -- see itk_upload.py / sipm_upload.py
### / fram_upload.py / delphi_upload.py for the actual entry points and
### their usage examples (each calls bulk_async.main() after filling in
### that model's default paths).
#
# python3 sipm_upload.py --environment test1 --dry-run
# python3 sipm_upload.py --environment local
# python3 itk_upload.py --environment production --max-concurrency 4
#
# Or drive bulk_async.py's CLI directly with an explicit --adapter:
# python3 -c "import bulk_async; bulk_async.main()" \
#     --adapter sipm --metadata-dir ... --data-root ... --environment local

###
