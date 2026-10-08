from __future__ import annotations

import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .converter import Converter
from .paths import PrefixError, output_relative_path

LOG = logging.getLogger("fram_converter")


@dataclass(frozen=True)
class Result:
    source: Path
    destination: Path
    status: str
    input_size: int = 0
    output_size: int = 0
    error: str | None = None

    @property
    def saved(self) -> int:
        return self.input_size - self.output_size

    @property
    def ratio(self) -> float | None:
        return (
            self.input_size / self.output_size
            if self.output_size > 0
            else None
        )


class ConversionRunner:
    def __init__(
        self,
        root: Path,
        destination: Path,
        converter: Converter,
        *,
        prefix: str = "data3",
        extensions: tuple[str, ...] = (".fits", ".fit", ".fts"),
        overwrite: bool = False,
        dry_run: bool = False,
        strict_prefix: bool = True,
        workers: int = 1,
        temp_dir: Path | None = None,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")

        self.root = root.resolve()
        self.destination = destination.resolve()
        self.converter = converter
        self.prefix = prefix
        self.extensions = tuple(
            e.lower() if e.startswith(".") else "." + e.lower()
            for e in extensions
        )
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.strict_prefix = strict_prefix
        self.workers = workers
        self.temp_dir = temp_dir.resolve() if temp_dir else None

        # Critical safety check: never let destination be scanned as input.
        try:
            self.destination.relative_to(self.root)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"destination must not be inside root: "
                f"{self.destination} is inside {self.root}"
            )

    def files(self) -> Iterable[Path]:
        if not self.root.is_dir():
            raise NotADirectoryError(self.root)

        for path in self.root.rglob("*"):
            if path.is_file() and path.suffix.lower() in self.extensions:
                yield path

    def destination_for(self, source: Path) -> Path:
        try:
            rel = output_relative_path(source, self.root, self.prefix)
        except PrefixError:
            if self.strict_prefix:
                raise
            rel = source.resolve().relative_to(self.root)

        # Deliberately keep the original .fits filename.
        return self.destination / rel

    def convert_one(self, source: Path) -> Result:
        input_size = source.stat().st_size
        target = self.destination_for(source)

        if target.exists() and not self.overwrite:
            LOG.info("SKIP %s -> %s (already exists)", source, target)
            return Result(source, target, "skipped", input_size=input_size)

        if self.dry_run:
            LOG.info("DRY-RUN %s -> %s", source, target)
            return Result(source, target, "dry-run", input_size=input_size)

        target.parent.mkdir(parents=True, exist_ok=True)

        try:
            # All fpack work happens in a private temporary directory.
            # The original source path is NEVER passed to the converter.
            with tempfile.TemporaryDirectory(
                prefix="fram-convert-",
                dir=str(self.temp_dir) if self.temp_dir else None,
            ) as td:
                td_path = Path(td)
                temp_input = td_path / source.name
                temp_output = td_path / source.name

                shutil.copy2(source, temp_input)

                # Safety assertion: converter gets only temporary paths.
                if temp_input.resolve() == source.resolve():
                    raise RuntimeError("internal safety check failed: temporary input equals source")

                self.converter.convert(temp_input, temp_output)

                if not temp_output.is_file():
                    raise RuntimeError(
                        f"converter produced no output: {temp_output}"
                    )

                output_size = temp_output.stat().st_size
                if output_size <= 0:
                    raise RuntimeError("converter produced an empty output")

                # os.replace publishes the completed file atomically.
                # No partially written target is visible.
                if target.exists():
                    if not self.overwrite:
                        raise FileExistsError(target)
                    target.unlink()

                os.replace(temp_output, target)

            ratio = input_size / output_size
            LOG.info(
                "OK %s -> %s (%.2f MiB -> %.2f MiB, %.2fx)",
                source,
                target,
                input_size / 1024**2,
                output_size / 1024**2,
                ratio,
            )
            return Result(source, target, "converted", input_size, output_size)

        except Exception as exc:
            LOG.exception("FAILED %s -> %s: %s", source, target, exc)
            return Result(
                source, target, "failed",
                input_size=input_size,
                error=str(exc),
            )

    def run(self) -> list[Result]:
        sources = list(self.files())
        LOG.info("Found %d FITS files", len(sources))

        results: list[Result] = []

        with ThreadPoolExecutor(
            max_workers=self.workers,
            thread_name_prefix="fits-worker",
        ) as executor:
            futures = {
                executor.submit(self.convert_one, source): source
                for source in sources
            }

            for future in as_completed(futures):
                results.append(future.result())

        return sorted(results, key=lambda r: str(r.source))
