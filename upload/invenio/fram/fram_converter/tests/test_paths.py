from pathlib import Path

from fram_converter.paths import output_relative_path


def test_root_above_data3():
    root = Path("/srv/archive")
    source = Path("/srv/archive/data3/2025/01/image.fits")
    assert output_relative_path(source, root) == Path(
        "data3/2025/01/image.fits"
    )


def test_root_is_data3():
    root = Path("/srv/archive/data3")
    source = Path("/srv/archive/data3/2025/01/image.fits")
    assert output_relative_path(source, root) == Path(
        "data3/2025/01/image.fits"
    )
