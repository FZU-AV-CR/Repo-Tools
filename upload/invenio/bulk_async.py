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
bounded concurrency, a circuit breaker, graceful shutdown on SIGINT/SIGTERM,
periodic progress logging, and a --dry-run mode that skips repository
client setup entirely (no network/token needed).

Differences from fram_bulk_async.py:
  - Work items are discovered by <adapter>.discover_items(), which for the
    current TXT/ZIP-based adapters (ITk, SiPM) takes --metadata-dir/
    --data-root instead of a single --input-dir. A future FRAM adapter
    doing a recursive filesystem walk for FITS files is free to interpret
    those same two flags however makes sense for it (e.g. --data-root as
    the FITS tree root and --metadata-dir unused).
  - Everything else (resume/dedup keyed off "key" in the stats CSV, circuit
    breaker, progress tracker, retry wrapper, shutdown handling) is
    unchanged in shape from fram_bulk_async.py.

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
from pathlib import Path

import adapters
import async_upload

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = {"ok", "failed", "skipped_invalid", "dryrun"}


# ============================================================
# RESUME / INTERRUPTED-RUN DETECTION
# (same logic as fram_bulk_async.py's _scan_stats -- generic since it's
# already keyed off the "key" column, not anything FITS-specific)
# ============================================================


def _scan_stats(stats_path: Path) -> tuple[set[str], set[str]]:
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
    """
    if not stats_path.exists():
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


# ============================================================
# CIRCUIT BREAKER  (unchanged from fram_bulk_async.py)
# ============================================================


class CircuitBreaker:
    """Stops new uploads from starting after too many consecutive failures.
    Does not cancel work already in flight."""

    def __init__(self, max_consecutive_failures: int):
        self.max_consecutive_failures = max_consecutive_failures
        self._consecutive_failures = 0
        self.tripped = False

    def record_success(self) -> None:
        self._consecutive_failures = 0

    def record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.max_consecutive_failures and not self.tripped:
            self.tripped = True
            logger.error(
                "Circuit breaker tripped after %s consecutive failures -- no new "
                "uploads will be started. Investigate connectivity/auth/schema "
                "before rerunning.",
                self._consecutive_failures,
            )


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
    stats_path: Path,
    stats_format: str,
    schema_url: str,
    dry_run: bool,
    validate: bool,
    stop_event: asyncio.Event,
    retries: int = 3,
    delay: int = 2,
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
    async_upload.setup_logging(log_file=Path(args.log_file) if args.log_file else None)

    # Resolve + configure the metadata-model adapter (--adapter flag ->
    # PHYSICS_ADAPTER env var -> error). async_upload.configure_adapter()
    # sets its own module-level `adapter` global (used by
    # upload_record_async()) and returns the same module here for local use.
    adapter = async_upload.configure_adapter(args.adapter)
    schema_url = args.schema_url or adapter.DEFAULT_SCHEMA_URL

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
    stats_path = Path(args.stats_path)

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

    logger.info(
        "Items to upload: %s (adapter=%s, environment=%s, max_concurrency=%s, dry_run=%s)",
        len(items), adapter.__name__, args.environment, args.max_concurrency, args.dry_run,
    )

    sem = asyncio.Semaphore(args.max_concurrency)
    breaker = CircuitBreaker(args.max_consecutive_failures)
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
            if stop_event.is_set() or breaker.tripped:
                logger.info("[%s] Skipping (shutdown requested or circuit breaker tripped)", item.key)
                progress.record("skipped")
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
                )
                breaker.record_success()
                progress.record("ok" if result is not None else "skipped")
                return result
            except Exception as exc:
                breaker.record_failure()
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
        "%s finished in %.2fs. total=%s ok=%s failed=%s skipped=%s (see %s for full per-item status)",
        "Dry run" if args.dry_run else "Bulk upload",
        time.perf_counter() - start,
        progress.done, progress.ok, progress.failed, progress.skipped,
        stats_path,
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
        default="upload_stats.csv",
        help="Path to the stats CSV (also used for resume)",
    )
    parser.add_argument("--max-concurrency", type=int, default=4, help="Maximum concurrent uploads")
    parser.add_argument(
        "--environment", choices=sorted(async_upload.ENVIRONMENTS), default="local",
        help="Which repository to upload to (local / test1 / production)",
    )
    parser.add_argument(
        "--schema-url", default=None,
        help="Invenio $schema value; defaults to the selected adapter's own DEFAULT_SCHEMA_URL if omitted. "
             "Confirmed only for local so far -- verify before using with test1/production.",
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
    parser.add_argument("--max-consecutive-failures", type=int, default=15, help="Stop starting new uploads after this many consecutive failures")
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
