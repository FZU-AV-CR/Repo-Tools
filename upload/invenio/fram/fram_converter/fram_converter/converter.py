from __future__ import annotations

import shutil
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path


class Converter(ABC):
    @abstractmethod
    def convert(self, source: Path, destination: Path) -> None:
        '''Convert a temporary input into a temporary output.'''


class CopyConverter(Converter):
    '''Testing converter. It does not invoke fpack.'''

    def convert(self, source: Path, destination: Path) -> None:
        shutil.copy2(source, destination)


class FpackConverter(Converter):
    '''
    Safe adapter for the external fpack executable.

    `source` must be a temporary copy, never the user's original FITS.

    fpack creates a compressed file next to the temporary source. The result
    is then moved to the caller-provided temporary destination. The caller
    later publishes that file atomically under its final `.fits` name.
    '''

    def __init__(self, executable: str = "fpack"):
        self.executable = executable

    def convert(self, source: Path, destination: Path) -> None:
        cmd = [self.executable, "-F", str(source)]
        completed = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        if completed.returncode != 0:
            details = (completed.stderr or completed.stdout).strip()
            raise RuntimeError(
                f"fpack failed with exit code {completed.returncode}"
                + (f": {details}" if details else "")
            )

        # fpack -F produces the compressed FITS as source + ".fz".
        packed = Path(str(source) + ".fz")

        if not packed.is_file():
            raise RuntimeError(
                f"fpack returned success but output was not found: {packed}"
            )

        if packed.stat().st_size <= 0:
            raise RuntimeError("fpack produced an empty output")

        # The temporary result is intentionally renamed to the final
        # destination name later by ConversionRunner. The original FITS is
        # still untouched.
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(packed), str(destination))
