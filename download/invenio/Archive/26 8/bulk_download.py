"""
Generic bulk async orchestrator for Physics-repository downloads.

This is the shared engine, not a standalone script -- it's driven by a
model-specific entry point (fram_download.py, delphi_download.py,
sipm_download.py, itk_download.py, ...), which fills in that model's
MODEL_NAME/default output dir and calls bulk_download.main(). Running this
file directly (`python3 bulk_download.py`) raises an error pointing to the
correct entry point instead -- same convention as bulk_async.py.

Mirrors bulk_async.py as closely as the download direction allows: resume
via a stats CSV, bounded concurrency, a shared transfer-weight budget, the
same rate/cooldown circuit breaker, graceful shutdown on SIGINT/SIGTERM,
periodic progress logging, and a --dry-run mode that skips repository
client setup entirely (no network/token needed).

Differences from bulk_async.py:
  - "items" here are individual FILES (see async_download.py's module
    docstring for why), discovered by first selecting records (--ids/
    --ids-file, or a --query/--filter/--year/... search) and then listing
    each record's files, rather than a filesystem walk.
  - record selection + file listing (async_download.resolve_records() /
    list_download_items()) needs the repository client even for a
    --dry-run *count*, unlike upload's filesystem-based discover_items();
    --dry-run here still performs record/file listing (read-only calls)
    but skips every actual download.
  - CircuitBreaker/ProgressTracker/shutdown-handling/"final outcome"
    reporting are reproduced here rather than imported from bulk_async.py,
    so that Download/ stays deployable without also requiring Upload/ on
    sys.path (same reasoning as async_download.py's WeightedSemaphore).
    Keep the two in sync by hand if one is improved.

The adapter itself is resolved at runtime by download_adapters.py, from
--adapter (this file's own CLI flag) or the PHYSICS_DOWNLOAD_ADAPTER
environment variable. Each model's entry-point script sets one of these
for you. To add a new metadata model, see download_adapters.py -- no
changes needed here.
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

import async_download
import download_adapters
from async_download import DownloadItem, WeightedSemaphore

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"ok", "failed", "skipped_exists", "dryrun"}


# ============================================================
# RESUME / INTERRUPTED-RUN DETECTION  (same logic as bulk_async.py's
# _scan_stats, keyed off the "key" column -- here key = "record:file")
# ============================================================


def _scan_stats(stats_path: Path) -> tuple[set[str], set[str]]:
    """Return (done_keys, interrupted_keys).

    done_keys: keys with a terminal status of 'ok' or 'skipped_exists' --
    safe to skip on resume (both mean the file is correctly present
    locally right now).
    interrupted_keys: keys with a 'started' row but no terminal row at
    all -- surfaced as a startup warning, then re-attempted (downloads are
    idempotent: FileSink truncates and rewrites the target, so a partial
    file from a crashed run is simply overwritten, unlike upload's
    orphaned-draft-record concern).
    """
    if not stats_path.exists():
        return set(), set()

    started: set[str] = set()
    terminal: set[str] = set()
    done: set[str] = set()

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
                    if status in ("ok", "skipped_exists"):
                        done.add(key)
    except Exception as exc:
        logger.warning("Failed to parse stats file %s: %s", stats_path, exc)
        return set(), set()

    interrupted = started - terminal
    return done, interrupted


def _final_rows_per_key(stats_path: Path) -> dict[str, dict]:
    """Return the LAST non-'started' stats row seen for each key. Same
    rationale as bulk_async.py's version: with retries, a key can have
    several terminal rows, and the last one chronologically is its true
    final outcome."""
    if not stats_path.exists():
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


def summarize_final_outcomes(stats_path: Path) -> dict[str, list[str]]:
    """Bucket every key's true final outcome by status, e.g.
    {"ok": [...], "failed": [...], "skipped_breaker": [...]}."""
    buckets: dict[str, list[str]] = {}
    for key, row in _final_rows_per_key(stats_path).items():
        status = str(row.get("status", "")).strip().lower() or "unknown"
        buckets.setdefault(status, []).append(key)
    return buckets


# ============================================================
# CIRCUIT BREAKER  (identical design to bulk_async.py's -- see its
# docstring for the full state-machine rationale)
# ============================================================


class CircuitBreaker:
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
        self._events: deque[tuple[float, bool]] = deque()
        self._opened_at: float | None = None
        self._half_open_results: list[bool] = []
        self._half_open_in_flight = 0
        self._lock = asyncio.Lock()

    @property
    def tripped(self) -> bool:
        return self._state == self.OPEN

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    async def allow_start(self) -> bool:
        async with self._lock:
            now = time.monotonic()

            if self._state == self.OPEN:
                if self._opened_at is not None and (now - self._opened_at) >= self.cooldown_seconds:
                    self._state = self.HALF_OPEN
                    self._half_open_results = []
                    self._half_open_in_flight = 0
                    logger.warning(
                        "Circuit breaker cooldown elapsed (%.0fs) -- entering half-open state, "
                        "allowing up to %s probe download(s) to test recovery.",
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
                            "closing, resuming normal downloads.",
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
                        "Circuit breaker tripped: %s/%s downloads failed in the last %.0fs "
                        "(>= %.0f%% failure-rate threshold). Pausing new downloads for %.0fs "
                        "before probing recovery. Investigate connectivity/auth/storage.",
                        failures, len(self._events), self.window_seconds,
                        self.failure_threshold * 100, self.cooldown_seconds,
                    )
                    self._state = self.OPEN
                    self._opened_at = now


# ============================================================
# PROGRESS TRACKING  (unchanged from bulk_async.py)
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
# GRACEFUL SHUTDOWN  (unchanged from bulk_async.py)
# ============================================================


async def _write_skip_stats(stats_path: Path, key: str, record_id: str, file_key: str, reason: str) -> None:
    """Visible stats row for an item never attempted because the circuit
    breaker had tripped or shutdown was requested. Deliberately NOT one of
    TERMINAL_STATUSES, so it's re-attempted (not treated as resolved) on
    the next run."""
    await async_download._write_stats(
        stats_path, "csv",
        async_download._stats_payload(
            key, record_id=record_id, file_key=file_key,
            status=f"skipped_{reason}", error=reason, attempt=0, max_attempts=0,
        ),
    )


def _request_shutdown(stop_event: asyncio.Event, sig) -> None:
    if not stop_event.is_set():
        logger.warning(
            "Received signal %s -- will not start new downloads; waiting for "
            "in-flight downloads to finish. This may take a while under high "
            "concurrency; use a process manager to force-kill if needed.",
            getattr(sig, "name", sig),
        )
        stop_event.set()


# ============================================================
# PER-ITEM RETRY WRAPPER  (unchanged shape from bulk_async.py)
# ============================================================


async def _download_with_retries(
    client,
    item: DownloadItem,
    stats_path: Path,
    dry_run: bool,
    verify_checksum: bool,
    multipart_threshold: int,
    parts: int,
    part_size: int | None,
    stop_event: asyncio.Event,
    retries: int = 3,
    delay: int = 2,
    transfer_limiter: WeightedSemaphore | None = None,
):
    retries = max(1, retries)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        if stop_event.is_set():
            logger.info("[%s] Shutdown requested, aborting before attempt %s", item.key, attempt)
            return None
        try:
            return await async_download.download_file_async(
                client=client,
                item=item,
                stats_path=stats_path,
                dry_run=dry_run,
                verify_checksum=verify_checksum,
                multipart_threshold=multipart_threshold,
                parts=parts,
                part_size=part_size,
                transfer_limiter=transfer_limiter,
                attempt=attempt,
                max_attempts=retries,
            )
        except Exception as exc:
            last_exc = exc
            logger.warning("[%s] Download failed (attempt %s/%s): %s", item.key, attempt, retries, exc)
            if attempt == retries:
                raise
            await asyncio.sleep(delay * attempt)
    raise last_exc  # pragma: no cover


# ============================================================
# MAIN
# ============================================================


async def main_async(args: argparse.Namespace) -> None:
    async_download.setup_logging(log_file=Path(args.log_file) if args.log_file else None)

    adapter = download_adapters.load(args.adapter)
    logger.info("Using adapter: %s", adapter.__name__)
    model = args.model_name or adapter.MODEL_NAME
    query = async_download.build_query(
        query=args.query or getattr(adapter, "DEFAULT_QUERY", None),
        filters=args.filter, ranges=args.filter_range, regexes=args.filter_regex, year=args.year,
        created_after=args.created_after, created_before=args.created_before,
    )

    if args.token:
        logger.warning(
            "Using --token from the command line; prefer the %s environment "
            "variable to avoid leaking credentials via process listings or "
            "shell history.",
            async_download.TOKEN_ENV_VAR,
        )

    output_dir = Path(args.output_dir or adapter.DEFAULT_OUTPUT_DIR)
    stats_path = Path(args.stats_path or (output_dir / "download_stats.csv"))
    metadata_dir = None if args.no_metadata else (output_dir / "_metadata")

    ids: list[str] = []
    if args.ids:
        ids.extend(p.strip() for p in args.ids.split(",") if p.strip())
    if args.ids_file:
        ids.extend(line.strip() for line in Path(args.ids_file).read_text().splitlines() if line.strip())

    # Preflight: build a client / confirm we have a token before doing any
    # record selection. Unlike upload's --dry-run (pure filesystem walk,
    # no client needed at all), a download --dry-run still needs the
    # client to select+list records/files -- it only skips the transfers
    # themselves (see async_download.download_file_async()'s dry_run branch).
    try:
        client = await async_download.create_client_for_environment(
            args.environment, token=args.token, confirm_production=not args.yes,
        )
    except Exception as exc:
        logger.error("Preflight failed -- could not create repository client: %s", exc)
        return

    records = await async_download.resolve_records(client, model=model, ids=ids or None, query=query)
    logger.info("Records selected (model=%s): %s", model, len(records))
    if not records:
        logger.info("Nothing to do.")
        return

    items = await async_download.list_download_items(
        client, records, output_dir=output_dir, metadata_dir=metadata_dir,
    )
    logger.info("Files to consider: %s", len(items))
    if not items:
        logger.info("Nothing to do.")
        return

    done_keys, interrupted_keys = _scan_stats(stats_path)
    if done_keys:
        before = len(items)
        items = [it for it in items if it.key not in done_keys]
        logger.info("Resume: skipped %s already-downloaded file(s) from %s", before - len(items), stats_path)

    if interrupted_keys:
        sample = ", ".join(list(interrupted_keys)[:20])
        logger.warning(
            "%s file(s) have a 'started' stats row from a previous run with no terminal "
            "status -- these will simply be re-downloaded (downloads are safe to overwrite, "
            "unlike upload's orphaned-draft-record concern). Examples: %s%s",
            len(interrupted_keys), sample, " ..." if len(interrupted_keys) > 20 else "",
        )

    logger.info(
        "Files to download: %s (adapter=%s, environment=%s, max_concurrency=%s, "
        "transfer_weight_budget=%s, multipart_threshold=%s bytes, parts=%s, "
        "breaker=[window=%ss min_samples=%s failure_threshold=%.0f%% cooldown=%ss "
        "half_open_probes=%s], dry_run=%s)",
        len(items), adapter.__name__, args.environment, args.max_concurrency,
        args.transfer_weight_budget, args.multipart_threshold, args.parts,
        args.breaker_window_seconds, args.breaker_min_samples, args.breaker_failure_threshold * 100,
        args.breaker_cooldown_seconds, args.breaker_half_open_probes, args.dry_run,
    )

    sem = asyncio.Semaphore(args.max_concurrency)
    transfer_limiter = WeightedSemaphore(args.transfer_weight_budget)
    breaker = CircuitBreaker(
        window_seconds=args.breaker_window_seconds,
        min_samples=args.breaker_min_samples,
        failure_threshold=args.breaker_failure_threshold,
        cooldown_seconds=args.breaker_cooldown_seconds,
        half_open_max_probes=args.breaker_half_open_probes,
        half_open_success_threshold=args.breaker_half_open_success_threshold,
    )
    progress = ProgressTracker(total=len(items), interval=args.progress_interval)
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown, stop_event, sig)
        except NotImplementedError:
            pass

    start = time.perf_counter()

    async def _run_one(item: DownloadItem):
        async with sem:
            if stop_event.is_set() or not await breaker.allow_start():
                reason = "shutdown" if stop_event.is_set() else "breaker"
                logger.info("[%s] Skipping (shutdown requested or circuit breaker open)", item.key)
                progress.record("skipped")
                if not args.dry_run:
                    await _write_skip_stats(stats_path, item.key, item.record_id, item.file_key, reason)
                return None
            try:
                result = await _download_with_retries(
                    client=client,
                    item=item,
                    stats_path=stats_path,
                    dry_run=args.dry_run,
                    verify_checksum=args.verify_checksum,
                    multipart_threshold=args.multipart_threshold,
                    parts=args.parts,
                    part_size=args.part_size,
                    stop_event=stop_event,
                    retries=args.max_retries,
                    delay=args.retry_delay,
                    transfer_limiter=transfer_limiter,
                )
                await breaker.record_result(success=True)
                progress.record("ok" if result else "skipped")
                return result
            except Exception as exc:
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

    if hasattr(client, "close"):
        await client.close()

    logger.info(
        "%s finished in %.2fs. total=%s ok=%s failed=%s skipped=%s (see %s for full per-item status)",
        "Dry run" if args.dry_run else "Bulk download",
        time.perf_counter() - start,
        progress.done, progress.ok, progress.failed, progress.skipped,
        stats_path,
    )

    if not args.dry_run:
        outcomes = summarize_final_outcomes(stats_path)
        failed_keys = outcomes.get("failed", [])
        if failed_keys:
            sample = ", ".join(failed_keys[:20])
            logger.error(
                "%s file(s) FAILED after exhausting all retries (see %s, status=='failed' rows with "
                "attempt==max_attempts, for full error details). Examples: %s%s",
                len(failed_keys), stats_path, sample, " ..." if len(failed_keys) > 20 else "",
            )
        skipped_breaker = outcomes.get("skipped_breaker", [])
        skipped_shutdown = outcomes.get("skipped_shutdown", [])
        if skipped_breaker:
            logger.warning(
                "%s file(s) were never attempted because the circuit breaker was open -- "
                "not failures, will be retried on the next run.",
                len(skipped_breaker),
            )
        if skipped_shutdown:
            logger.warning(
                "%s file(s) were never attempted due to shutdown -- not failures, will be "
                "retried on the next run.",
                len(skipped_shutdown),
            )
        if not failed_keys and not skipped_breaker and not skipped_shutdown:
            logger.info("No failed or breaker/shutdown-skipped files in %s.", stats_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk async download of Physics-repository records + files. "
                     "The metadata model is selected via --adapter (or the PHYSICS_DOWNLOAD_ADAPTER env var)."
    )
    parser.add_argument(
        "--adapter", choices=download_adapters.available(), default=None,
        help=f"Which metadata-model adapter to use. Falls back to the {download_adapters.ENV_VAR} "
             "environment variable if omitted (each model's own entry-point script, e.g. "
             "fram_download.py, sets this for you).",
    )
    parser.add_argument("--model-name", default=None,
                         help="Override the adapter's MODEL_NAME (repository `model` search param).")

    sel = parser.add_argument_group("record selection (choose one, or combine query + filters)")
    sel.add_argument("--ids", default=None, help="Comma-separated list of record PIDs to download.")
    sel.add_argument("--ids-file", default=None, help="Path to a text file with one record PID per line.")
    sel.add_argument("--query", default=None,
                      help="Free-text OpenSearch query, ANDed with the adapter's DEFAULT_QUERY if any.")
    sel.add_argument("--filter", action="append", default=[], metavar="FIELD=VALUE",
                      help="Exact keyword match, ANDed into the query. Repeatable, e.g. "
                           "--filter metadata.site=LaPalma --filter metadata.experiment=FRAM")
    sel.add_argument("--filter-range", action="append", default=[], metavar="FIELD=MIN:MAX",
                      help="Numeric or date range filter, ANDed into the query. Repeatable. Either bound "
                           "may be left empty for an open range, e.g. --filter-range metadata.alt_az.azimuth=100:200 "
                           "or --filter-range metadata.exposure=:60")
    sel.add_argument("--filter-regex", action="append", default=[], metavar="FIELD=REGEX",
                      help="Regex match on a keyword field, ANDed into the query. Repeatable, e.g. "
                           "--filter-regex metadata.target='M[0-9]+' . OpenSearch/Lucene regex syntax "
                           "(similar to but not identical to Python re -- no lookaround); escape a literal "
                           "'/' in the pattern as \\/.")
    sel.add_argument("--year", type=int, default=None, help="Convenience filter: records created in this year.")
    sel.add_argument("--created-from", "--created-after", dest="created_after", default=None,
                      help="Only records created on/after this ISO date (inclusive), e.g. 2026-01-01")
    sel.add_argument("--created-to", "--created-before", dest="created_before", default=None,
                      help="Only records created on/before this ISO date (inclusive), e.g. 2026-12-31")

    parser.add_argument("--output-dir", default=None,
                         help="Where downloaded files go; defaults to the adapter's DEFAULT_OUTPUT_DIR.")
    parser.add_argument("--stats-path", default=None,
                         help="Path to the stats CSV (also used for resume); defaults to <output-dir>/download_stats.csv.")
    parser.add_argument("--no-metadata", action="store_true",
                         help="Don't save each record's metadata JSON to <output-dir>/_metadata/.")

    parser.add_argument("--max-concurrency", type=int, default=8, help="Maximum concurrent file downloads")
    parser.add_argument(
        "--transfer-weight-budget", type=int, default=50,
        help="Shared transfer budget across all in-flight file downloads. Small/single-stream transfers "
             "consume 1 unit; large/multi-part transfers (see --multipart-threshold) consume 5 (see "
             "async_download.py's MULTIPART_WEIGHT). Independent of --max-concurrency, which only bounds "
             "how many files' downloads overlap -- this is what actually bounds load on the repository/"
             "network. Confirm a suitable value against the target environment (test1/production) with "
             "Cesnet before scaling up, especially for large bulk FRAM runs.",
    )
    parser.add_argument(
        "--multipart-threshold", type=int, default=async_download.MULTIPART_THRESHOLD_BYTES,
        help=f"File size in bytes above which multi-part parallel download is used "
             f"(default: {async_download.MULTIPART_THRESHOLD_BYTES}).",
    )
    parser.add_argument("--parts", type=int, default=async_download.DEFAULT_PARTS,
                         help=f"Number of parts for multi-part downloads (default: {async_download.DEFAULT_PARTS}).")
    parser.add_argument("--part-size", type=int, default=None,
                         help="Explicit part size in bytes for multi-part downloads (overrides --parts).")

    parser.add_argument(
        "--environment", choices=sorted(async_download.ENVIRONMENTS), default="local",
        help="Which repository to download from (local / test1 / production)",
    )
    parser.add_argument(
        "--token", default=None,
        help=f"API token; prefer the {async_download.TOKEN_ENV_VAR} environment variable over this flag",
    )
    parser.add_argument(
        "--yes", action="store_true",
        help="Skip the interactive confirmation prompt required for --environment production",
    )
    parser.add_argument("--dry-run", action="store_true",
                         help="Select records/list files but don't download anything")
    parser.add_argument("--verify-checksum", action="store_true",
                         help="Also verify existing local files by hashing them (slow at TB scale; "
                              "off by default, size-based resume check is used instead)")

    parser.add_argument(
        "--breaker-window-seconds", type=float, default=60.0,
        help="Circuit breaker: size of the sliding time window (seconds) over which the failure rate "
             "is evaluated.",
    )
    parser.add_argument(
        "--breaker-min-samples", type=int, default=20,
        help="Circuit breaker: minimum outcomes required within the window before the failure rate is "
             "evaluated at all. Scale this up with --max-concurrency.",
    )
    parser.add_argument(
        "--breaker-failure-threshold", type=float, default=0.5,
        help="Circuit breaker: failure rate (0.0-1.0) within the window that trips the breaker.",
    )
    parser.add_argument(
        "--breaker-cooldown-seconds", type=float, default=60.0,
        help="Circuit breaker: how long to block new downloads after tripping before probing recovery.",
    )
    parser.add_argument(
        "--breaker-half-open-probes", type=int, default=5,
        help="Circuit breaker: number of probe downloads allowed through after a cooldown.",
    )
    parser.add_argument(
        "--breaker-half-open-success-threshold", type=float, default=0.6,
        help="Circuit breaker: fraction of half-open probes that must succeed to close the breaker again.",
    )
    parser.add_argument("--max-retries", type=int, default=3, help="Retry attempts per file")
    parser.add_argument("--retry-delay", type=int, default=2, help="Base delay (seconds) between retries; scales with attempt number")
    parser.add_argument("--log-file", default=None, help="Optional rotating log file path, in addition to console output")
    parser.add_argument("--progress-interval", type=float, default=30.0, help="Seconds between progress log lines")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    raise SystemExit(
        "bulk_download.py is not meant to be run directly -- it's the shared "
        "engine, driven by a model-specific entry point. Run one of:\n"
        "    python3 fram_download.py [flags]\n"
        "    python3 delphi_download.py [flags]\n"
        "    python3 sipm_download.py [flags]\n"
        "    python3 itk_download.py [flags]\n"
        "(each imports this module and calls bulk_download.main() after filling "
        "in that model's default output dir and --adapter name.)"
    )
