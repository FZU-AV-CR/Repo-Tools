from pathlib import Path


class PrefixError(ValueError):
    pass


def output_relative_path(source: Path, root: Path, prefix: str = "data3") -> Path:
    """Return the anchor-relative path that starts at *prefix*.

    Three cases are handled:

    1. ``prefix`` appears somewhere in the path below ``root``::

           root=/mnt  source=/mnt/data3/auger/2017/x.fits
           -> data3/auger/2017/x.fits

    2. ``root`` ends exactly at ``prefix`` (root *is* the prefix dir)::

           root=/mnt/data3  source=/mnt/data3/auger/2017/x.fits
           -> data3/auger/2017/x.fits

    3. ``root`` is *inside* the prefix subtree (root is a subdirectory of
       the prefix directory, e.g. ``--root /mnt/data3/auger/2017``)::

           root=/mnt/data3/auger/2017  source=/mnt/data3/auger/2017/x.fits
           -> data3/auger/2017/x.fits

       In this case the prefix and the root-relative path are concatenated,
       but only the *suffix* of root's parts that starts at prefix is used,
       avoiding double-counting when root contains prefix more than once.
    """
    root = root.resolve()
    source = source.resolve()

    try:
        rel = source.relative_to(root)
    except ValueError as exc:
        raise PrefixError(f"{source} is outside root {root}") from exc

    parts = rel.parts
    prefix_parts = tuple(Path(prefix).parts)

    if not prefix_parts:
        raise PrefixError("prefix must not be empty")

    # Case 1: prefix appears in the relative path (root is above prefix dir).
    for i in range(len(parts) - len(prefix_parts) + 1):
        if parts[i:i + len(prefix_parts)] == prefix_parts:
            return Path(*parts[i:])

    # Cases 2 & 3: prefix is in root itself — find rightmost occurrence.
    root_parts = root.parts
    for i in range(len(root_parts) - len(prefix_parts), -1, -1):
        if root_parts[i:i + len(prefix_parts)] == prefix_parts:
            anchor = root_parts[i:]   # e.g. ("data3", "auger", "2017")
            return Path(*anchor, *parts)

    raise PrefixError(
        f"prefix {prefix!r} was not found in {source} relative to {root}"
    )
