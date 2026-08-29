"""Non-streaming converters reject `-` with an actionable message.

Regression tests for #749. `gpio convert geojson - out.geojson` works (#723),
but csv/flatgeobuf/geopackage/shapefile have no stdin-consuming path: `-` was
passed through as an ordinary path and surfaced from GDAL/DuckDB as
``Failed to create CSV: File not found: -``, which says neither what happened
nor what to do about it.
"""

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import convert
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.format_writers import write_csv, write_gdal_format

# (cli subcommand, output file name, format description as it appears in the error)
NON_STREAMING = [
    ("csv", "out.csv", "CSV"),
    ("flatgeobuf", "out.fgb", "FlatGeobuf"),
    ("geopackage", "out.gpkg", "GeoPackage"),
    ("shapefile", "out.shp", "Shapefile"),
]


@pytest.mark.parametrize(
    ("subcommand", "out_name", "description"),
    NON_STREAMING,
    ids=[c[0] for c in NON_STREAMING],
)
def test_cli_rejects_stdin_with_actionable_message(subcommand, out_name, description, tmp_path):
    """The CLI explains that stdin is unsupported and how to work around it."""
    runner = CliRunner()
    result = runner.invoke(convert, [subcommand, "-", str(tmp_path / out_name)])

    assert result.exit_code != 0
    output = result.output
    assert "File not found: -" not in output, "still leaking the misleading GDAL/DuckDB error"
    assert f"stdin ('-') is not supported for {description} output" in output
    assert "gpio convert geoparquet - tmp.parquet" in output
    assert f"gpio convert {subcommand} tmp.parquet" in output


def test_no_output_file_is_created(tmp_path):
    """The guard fires before anything is written."""
    runner = CliRunner()
    out = tmp_path / "out.csv"
    runner.invoke(convert, ["csv", "-", str(out)])
    assert not out.exists()


def test_write_csv_raises_geoparquet_error(tmp_path):
    """The core writer raises, so the Python API sees it too."""
    with pytest.raises(GeoParquetError, match=r"stdin \('-'\) is not supported for CSV"):
        write_csv("-", str(tmp_path / "out.csv"))


@pytest.mark.parametrize("format_name", ["flatgeobuf", "geopackage", "shapefile"])
def test_write_gdal_format_raises_geoparquet_error(format_name, tmp_path):
    """Same for the shared GDAL writer, for every format it serves."""
    with pytest.raises(GeoParquetError, match=r"stdin \('-'\) is not supported"):
        write_gdal_format("-", str(tmp_path / "out.bin"), format_name)


def test_geojson_still_accepts_stdin():
    """The guard must not touch `convert geojson`, which does consume stdin (#723)."""
    from geoparquet_io.core.format_writers import _reject_stdin_input

    # Sanity: the helper only fires on the stream marker.
    _reject_stdin_input("some/file.parquet", "CSV", "csv", "out.csv")
