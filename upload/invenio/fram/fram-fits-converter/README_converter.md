# fram-fits-converter 0.3.1

Safe recursive FITS compression using the external `fpack` utility.

## Safety guarantee

The original FITS files are **never modified**.

For every input file:

1. The original is copied to a private temporary directory.
2. Only the temporary copy is passed to `fpack`.
3. The `.fz` produced by `fpack` is checked.
4. It is published to `--destination` under the **same `.fits` filename**.
5. Publication uses an atomic filesystem replace; if source and destination
   are on different filesystems, a same-filesystem staging copy is used
   before the final atomic rename.
6. Temporary files are removed automatically.

The destination is also forbidden from being inside the source root. This
prevents the recursive scanner from discovering its own output.

## Example

    fram-convert \
      --root /srv/archive/data3 \
      --destination /srv/compressed \
      --workers 8

Input:

    /srv/archive/data3/2025/01/06/image.fits

Output:

    /srv/compressed/data3/2025/01/06/image.fits

The output has the same filename, but contains the FITS tile-compressed data
produced by `fpack`.

## Path prefix

The `--prefix` option (default: `data3`) controls the anchor that is
preserved in the output tree. The following `--root` forms are all
supported:

| `--root` | result in `--destination` |
|---|---|
| `/mnt` | `<dest>/data3/auger/2017/…` |
| `/mnt/data3` | `<dest>/data3/auger/2017/…` |
| `/mnt/data3/auger/2017` | `<dest>/data3/auger/2017/…` |

In every case the output path starts at the prefix, so the destination
tree mirrors the archive layout from `data3` downward regardless of how
deep `--root` points.

If the prefix is absent from the path entirely, a `PrefixError` is raised
(and the file is recorded as failed). Pass `--prefix ""` or restructure
`--root` to avoid this.

## fpack executable

By default the program runs `fpack` from PATH:

    fram-convert --root ... --destination ...

If it is elsewhere:

    fram-convert ... --fpack /opt/cfitsio/bin/fpack

## Temporary directory

By default Python uses the system temporary directory. For large FITS files
you may want a filesystem with sufficient free space:

    fram-convert ... --temp-dir /scratch/fram-convert

The temporary copy is deleted after each successful or failed conversion.

## Parallel processing

Use:

    --workers 8

The worker count controls the number of FITS conversions running in parallel.
Start conservatively because each worker needs temporary disk space and I/O.

## Existing output

By default an existing destination file is skipped.

To replace it:

    --overwrite

## Dry run

Inspect the planned input/output mapping without running fpack:

    fram-convert \
      --root /srv/archive/data3 \
      --destination /srv/compressed \
      --dry-run

## Statistics

The final summary reports:

- number of FITS files
- converted / skipped / failed
- total input size
- total output size
- saved space
- input/output compression ratio

Per-file log messages also contain input size, output size and ratio.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
