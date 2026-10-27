from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .converter import FpackConverter
from .runner import ConversionRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="fram-convert",
        description=(
            "Safely compress FRAM FITS files with fpack. "
            "Original files are never modified."
        ),
    )
    parser.add_argument("--root", required=True, type=Path,
                        help="Input tree to scan recursively.")
    parser.add_argument("--destination", required=True, type=Path,
                        help="Output root directory.")
    parser.add_argument("--prefix", default="data3",
                        help="Path prefix to preserve (default: data3).")
    parser.add_argument("--fpack", default="fpack",
                        help="Path/name of fpack executable (default: fpack).")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel conversions (default: 1).")
    parser.add_argument("--temp-dir", type=Path,
                        help="Directory for temporary working copies.")
    parser.add_argument("--overwrite", action="store_true",
                        help="Replace existing output files.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show operations without writing files.")
    parser.add_argument("--log-file", type=Path,
                        help="Write logs to this file as well.")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress INFO messages on the console.")
    return parser


def configure_logging(log_file: Path | None, quiet: bool) -> None:
    handlers: list[logging.Handler] = []

    console = logging.StreamHandler()
    console.setLevel(logging.WARNING if quiet else logging.INFO)
    handlers.append(console)

    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
        handlers=handlers,
    )


def main() -> int:
    args = build_parser().parse_args()
    configure_logging(args.log_file, args.quiet)

    try:
        runner = ConversionRunner(
            args.root,
            args.destination,
            FpackConverter(args.fpack),
            prefix=args.prefix,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
            workers=args.workers,
            temp_dir=args.temp_dir,
        )
        results = runner.run()
    except Exception as exc:
        logging.getLogger("fram_converter").error("%s", exc)
        return 2

    converted = [r for r in results if r.status == "converted"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "failed"]
    dry_run = [r for r in results if r.status == "dry-run"]

    input_size = sum(r.input_size for r in results)
    output_size = sum(r.output_size for r in converted)
    saved = input_size - output_size

    print()
    print("Summary")
    print(f"  FITS files:        {len(results)}")
    print(f"  Converted:         {len(converted)}")
    print(f"  Skipped:           {len(skipped)}")
    print(f"  Dry-run:           {len(dry_run)}")
    print(f"  Failed:            {len(failed)}")
    print(f"  Input size:        {input_size / 1024**3:.2f} GiB")
    print(f"  Output size:       {output_size / 1024**3:.2f} GiB")
    print(f"  Saved:             {saved / 1024**3:.2f} GiB")

    if output_size:
        print(f"  Compression ratio: {input_size / output_size:.2f}x")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
