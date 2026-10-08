from pathlib import Path


class PrefixError(ValueError):
    pass


def output_relative_path(source: Path, root: Path, prefix: str = "data3") -> Path:
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

    for i in range(len(parts) - len(prefix_parts) + 1):
        if parts[i:i + len(prefix_parts)] == prefix_parts:
            return Path(*parts[i:])

    # Also support --root ending exactly at the prefix.
    if root.parts[-len(prefix_parts):] == prefix_parts:
        return Path(*prefix_parts, *parts)

    raise PrefixError(
        f"prefix {prefix!r} was not found in {source} relative to {root}"
    )
