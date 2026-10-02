"""
Automated multi-pass wrapper around fram_upload.py: runs a pass, waits a
short buffer, runs another pass, and stops early once nothing is left to
retry -- rather than requiring you to manually re-invoke fram_upload.py
yourself to pick up items that failed on the first attempt.

WHY THIS WORKS WITHOUT REIMPLEMENTING RESUME LOGIC: bulk_async.py already
treats any two runs sharing the same --stats-path as one continuous job --
a rerun automatically skips everything already "ok" and naturally retries
anything "failed" (failed items simply aren't excluded from the next
run's item list). This wrapper's only real job is to run that a second
time automatically, with a sensible buffer in between, and to stop early
if pass 1 already finished everything -- it does not duplicate any of the
actual upload/retry/resume mechanics.

WHY A BUFFER BETWEEN PASSES AT ALL: most items that fail during pass 1
already get a substantial real-world gap before pass 2 reaches them,
simply because pass 1 keeps running for a while after any single item
fails. The one case a buffer specifically helps is the small tail of
items that fail right at the very end of pass 1 -- without a buffer,
those would get retried near-instantly. A short buffer (default 5
minutes) is cheap on a run that's already hours long, and gives even that
tail a moment for anything transient (server-side or otherwise) to clear.

USAGE: pass every fram_upload.py flag you'd normally use, plus this
script's own --passes/--buffer-minutes/--run-id if you want non-default
values. Everything not recognized by this wrapper is forwarded verbatim
to fram_upload.py -- no "--" separator needed (or wanted; one typed out
of habit is stripped automatically rather than forwarded literally).

    python3 fram_upload_with_retry.py \
        --environment test1 --data-root "..." --date-pattern "202204*" \
        --max-concurrency 4 --rate-limit "450000/3600" --rate-limit "45000/60" \
        --exclude-dirs "bad,darks,WF0"

    # non-default pass count / buffer:
    python3 fram_upload_with_retry.py --passes 3 --buffer-minutes 10 \
        --environment test1 --data-root "..."

--run-id/--stats-path are wrapper-controlled (stripped from anything you
pass through) so both passes are guaranteed to share the same stats file
-- that sharing is what makes resume/skip/retry work correctly across
passes at all. --log-file is likewise wrapper-controlled, but given a
distinct per-pass name (upload_pass1.log, upload_pass2.log, ...) so the
two passes' logs don't overwrite each other despite sharing a run-id.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# fram_upload.py / bulk_async.py / async_upload.py are assumed to live
# alongside this script and one directory up, respectively -- same layout
# fram_upload.py itself already assumes (see its own ENGINE_DIR).
THIS_DIR = Path(__file__).resolve().parent
ENGINE_DIR = THIS_DIR.parent
if str(ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(ENGINE_DIR))

import bulk_async  # noqa: E402  (path insertion above must happen first)

DEFAULT_FRAM_UPLOAD_SCRIPT = THIS_DIR / "fram_upload.py"
TOKEN_ENV_VAR = "INVENIO_TOKEN"  # must match async_upload.py's own constant


def _extract_flag_value(argv: list[str], flag: str) -> str | None:
    """Same as fram_upload.py's own helper: last-occurrence-wins value for
    `flag` in either "--flag value" or "--flag=value" form."""
    value = None
    for i, arg in enumerate(argv):
        if arg == flag and i + 1 < len(argv):
            value = argv[i + 1]
        elif arg.startswith(flag + "="):
            value = arg.split("=", 1)[1]
    return value


def _strip_flag(argv: list[str], flag: str) -> list[str]:
    """Same as fram_upload.py's own helper: remove every occurrence of
    `flag` (and its value) from argv."""
    result: list[str] = []
    skip_next = False
    for arg in argv:
        if skip_next:
            skip_next = False
            continue
        if arg == flag:
            skip_next = True
            continue
        if arg.startswith(flag + "="):
            continue
        result.append(arg)
    return result


def _outcome_counts(stats_path: Path) -> dict[str, int]:
    outcomes = bulk_async.summarize_final_outcomes(stats_path)
    return {status: len(keys) for status, keys in outcomes.items()}


def _remaining_count(counts: dict[str, int]) -> int:
    return sum(n for status, n in counts.items() if status != "ok")


def _print_report(title: str, counts: dict[str, int]) -> None:
    total = sum(counts.values())
    print(f"\n=== {title} ===")
    if total == 0:
        print("(no terminal rows yet)")
        return
    for status in sorted(counts, key=lambda s: (s != "ok", s)):
        print(f"  {status:20s} {counts[status]}")
    print(f"  {'total':20s} {total}")


def parse_args() -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description="Run fram_upload.py for up to --passes attempts, with a --buffer-minutes "
                     "pause between passes, stopping early once nothing remains to retry. All "
                     "other flags are forwarded to fram_upload.py unchanged.",
        allow_abbrev=False,
    )
    parser.add_argument("--passes", type=int, default=2, help="Maximum number of passes (default: 2).")
    parser.add_argument(
        "--buffer-minutes", type=float, default=5.0,
        help="Minutes to wait between passes (default: 5). Skipped after the final pass, and "
             "skipped entirely if a pass already leaves nothing left to retry.",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="Shared run folder name (<cwd>/logs/<run-id>/) for both passes' stats CSV and logs. "
             "Defaults to a fresh timestamp if omitted, exactly like fram_upload.py's own default.",
    )
    parser.add_argument(
        "--stats-path", default=None,
        help="Explicit stats CSV path shared by every pass. Overrides --run-id's default location "
             "if given. An explicit --stats-path passed through in fram_upload.py's own flags is "
             "ignored -- this wrapper needs to control it directly for resume to work across passes.",
    )
    parser.add_argument(
        "--fram-upload-script", default=str(DEFAULT_FRAM_UPLOAD_SCRIPT),
        help=f"Path to fram_upload.py (default: {DEFAULT_FRAM_UPLOAD_SCRIPT}).",
    )
    parser.add_argument(
        "--token", default=None,
        help=f"API token, forwarded explicitly to every pass. Prefer the {TOKEN_ENV_VAR} "
             "environment variable instead (already inherited automatically by every pass -- "
             "no wrapper flag needed for that case). This flag exists for cases where the env "
             "var genuinely isn't set. Either way, see the upfront token check in main(): a "
             "missing token for a real (non-local, non-dry-run) run is refused before pass 1 "
             "even starts, rather than risking pass 2 hanging on an interactive prompt with "
             "nobody there to answer it after the buffer wait.",
    )
    return parser.parse_known_args()


def main() -> None:
    args, passthrough = parse_args()
    # A "--" separator isn't needed (unrecognized flags are forwarded
    # automatically), but if someone types it out of habit, argparse's
    # parse_known_args() doesn't consume it -- strip it defensively so it
    # never ends up forwarded to fram_upload.py's own argument parser.
    passthrough = [a for a in passthrough if a != "--"]

    if args.passes < 1:
        raise SystemExit("--passes must be at least 1")

    if _extract_flag_value(passthrough, "--disable-stats") is not None or "--disable-stats" in passthrough:
        raise SystemExit(
            "--disable-stats was passed through, but this wrapper fundamentally depends on a "
            "persistent stats CSV to know what happened between passes. Remove --disable-stats, "
            "or just run fram_upload.py directly if you genuinely don't want stats kept."
        )
    if "--dry-run" in passthrough:
        print(
            "WARNING: --dry-run was passed through. Dry-run items are never marked 'ok', so this "
            "wrapper's early-stop and 'remaining' tracking won't behave meaningfully -- every pass "
            "will look identical. Consider just running fram_upload.py directly for a dry run.",
            file=sys.stderr,
        )

    is_dry_run = "--dry-run" in passthrough
    environment = _extract_flag_value(passthrough, "--environment") or "local"
    token_available = bool(args.token) or bool(os.environ.get(TOKEN_ENV_VAR))
    if environment != "local" and not is_dry_run and not token_available:
        raise SystemExit(
            f"No API token available (checked --token and the {TOKEN_ENV_VAR} environment "
            f"variable) for --environment {environment}. Refusing to start: without one, pass 1 "
            f"would fall back to an interactive prompt, which works fine while you're watching it "
            f"-- but pass 2 runs unattended after the buffer wait, and would hang indefinitely "
            f"waiting for input nobody will provide. Set {TOKEN_ENV_VAR} or pass --token, then rerun."
        )

    # --run-id/--stats-path are this wrapper's OWN registered flags, so
    # argparse already captures every occurrence of them directly into
    # args.run_id/args.stats_path -- they can never end up in `passthrough`
    # at all. --log-file is NOT one of this wrapper's own flags, though, so
    # a passthrough --log-file needs to be caught and stripped explicitly
    # (this wrapper needs to control it to give each pass a distinct name).
    if _extract_flag_value(passthrough, "--log-file") is not None:
        print("NOTE: ignoring passthrough --log-file -- this wrapper controls it directly "
              "(each pass gets its own log file; see --run-id's folder).", file=sys.stderr)
        passthrough = _strip_flag(passthrough, "--log-file")

    run_id = args.run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path.cwd() / "logs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    stats_path = Path(args.stats_path) if args.stats_path else (run_dir / "upload_stats.csv")

    fram_upload_script = Path(args.fram_upload_script)
    if not fram_upload_script.exists():
        raise SystemExit(f"fram_upload.py not found at {fram_upload_script} -- pass --fram-upload-script.")

    print(f"Run id: {run_id}")
    print(f"Stats CSV (shared across all passes): {stats_path}")
    print(f"Passes: up to {args.passes}, buffer: {args.buffer_minutes} min between passes\n")

    for pass_num in range(1, args.passes + 1):
        log_path = run_dir / f"upload_pass{pass_num}.log"
        cmd = [
            sys.executable, str(fram_upload_script),
            "--stats-path", str(stats_path),
            "--log-file", str(log_path),
            *passthrough,
        ]
        if args.token:
            cmd += ["--token", args.token]
        # Redact the token from what gets printed/logged, even though it's
        # passed for real to subprocess.run() below -- printing it would
        # defeat the point of preferring the env var over --token in the
        # first place (shell history / process listings).
        cmd_display = ["<TOKEN>" if a == args.token else a for a in cmd] if args.token else cmd
        print(f"--- Pass {pass_num}/{args.passes} --- log: {log_path}")
        print("  " + " ".join(cmd_display))
        try:
            result = subprocess.run(cmd)
        except KeyboardInterrupt:
            print("\nInterrupted during a pass -- the completed work is safe in the stats CSV. "
                  "Rerun this same command (same --run-id) to resume.")
            raise SystemExit(130)

        if result.returncode != 0:
            print(
                f"WARNING: pass {pass_num} exited with code {result.returncode} (a crash, not just "
                f"per-item failures -- check {log_path}). Continuing to the next pass anyway, since "
                f"partial progress may still be recorded.",
                file=sys.stderr,
            )

        if not stats_path.exists():
            print(f"WARNING: no stats CSV found at {stats_path} after pass {pass_num} -- "
                  f"skipping remaining-count check for this pass.", file=sys.stderr)
            continue

        counts = _outcome_counts(stats_path)
        _print_report(f"After pass {pass_num}", counts)
        remaining = _remaining_count(counts)

        if remaining == 0:
            if pass_num < args.passes:
                print(f"\nNothing left to retry after pass {pass_num} -- stopping early "
                      f"(skipping remaining passes and the buffer wait).")
            else:
                print(f"\nNothing left to retry after pass {pass_num}.")
            break

        if pass_num < args.passes:
            print(f"\n{remaining} item(s) still not 'ok'. Waiting {args.buffer_minutes} "
                  f"minute(s) before pass {pass_num + 1}...")
            try:
                time.sleep(args.buffer_minutes * 60)
            except KeyboardInterrupt:
                print("\nInterrupted during the buffer wait -- completed work is safe. "
                      "Rerun this same command (same --run-id) to resume.")
                raise SystemExit(130)

    final_counts = _outcome_counts(stats_path) if stats_path.exists() else {}
    _print_report("FINAL RESULT (all passes)", final_counts)
    still_failing = _remaining_count(final_counts)
    if still_failing:
        print(
            f"\n{still_failing} item(s) still not 'ok' after all {args.passes} pass(es) -- "
            f"these likely need manual attention rather than another automated retry. "
            f"See {stats_path} (status != 'ok' rows) for details."
        )
        raise SystemExit(1)
    else:
        print("\nAll items succeeded.")


if __name__ == "__main__":
    main()
