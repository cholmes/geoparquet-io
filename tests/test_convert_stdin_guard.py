"""Non-streaming converters reject `-` with an actionable message.

Regression tests for #749. `gpio convert geojson - out.geojson` works (#723),
but csv/flatgeobuf/geopackage/shapefile have no stdin-consuming path: `-` was
passed through as an ordinary path and surfaced from GDAL/DuckDB as
``Failed to create CSV: File not found: -``, which says neither what happened
nor what to do about it.
"""

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli, convert
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.format_writers import (
    _reject_stdin_input,
    write_csv,
    write_gdal_format,
)

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
    assert "gpio extract - tmp.parquet" in output
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


def test_helper_does_not_fire_on_an_ordinary_path(tmp_path):
    """The guard keys on the stream marker alone, not on any path that looks odd."""
    for path in ("some/file.parquet", "./-", str(tmp_path / "-.parquet"), "-file.parquet"):
        _reject_stdin_input(path, "CSV", "csv", "out.csv")


def test_geojson_still_accepts_stdin(buildings_test_file, tmp_path):
    """The guard must not touch `convert geojson`, which does consume stdin.

    Asserted end to end rather than by inspecting the helper: `convert geojson`
    reaches a different writer, and the point is that a real stream still comes
    out the other side (#723, #746).
    """
    runner = CliRunner()
    stream = runner.invoke(convert, ["geoparquet", str(buildings_test_file), "-"])
    assert stream.exit_code == 0, stream.output

    out = tmp_path / "out.geojson"
    result = runner.invoke(convert, ["geojson", "-", str(out)], input=stream.stdout_bytes)

    assert result.exit_code == 0, result.output
    assert out.exists() and out.stat().st_size > 0


def test_the_suggested_workaround_actually_runs(buildings_test_file, tmp_path):
    """The two commands the error prints must work when run.

    The message used to suggest `gpio convert geoparquet - tmp.parquet`, which
    accepts `-` as an *output* only -- so following the advice produced the very
    `File not found: -` the message exists to replace. The commands are parsed
    back out of the message so the two cannot drift apart again (#749).
    """
    runner = CliRunner()
    message = runner.invoke(convert, ["csv", "-", str(tmp_path / "out.csv")]).output
    suggestion = message.rsplit("Materialize the stream first:", 1)[1].strip()
    materialize, then_convert = (part.strip().split() for part in suggestion.split("&&"))

    stream = runner.invoke(convert, ["geoparquet", str(buildings_test_file), "-"])
    assert stream.exit_code == 0, stream.output

    # gpio extract - tmp.parquet
    assert materialize[:2] == ["gpio", "extract"]
    tmp_parquet = tmp_path / "tmp.parquet"
    step_one = runner.invoke(cli, [*materialize[1:-1], str(tmp_parquet)], input=stream.stdout_bytes)
    assert step_one.exit_code == 0, step_one.output
    assert tmp_parquet.exists()

    # gpio convert csv tmp.parquet out.csv
    assert then_convert[:3] == ["gpio", "convert", "csv"]
    out_csv = tmp_path / "out.csv"
    step_two = runner.invoke(cli, [*then_convert[1:-2], str(tmp_parquet), str(out_csv)])
    assert step_two.exit_code == 0, step_two.output
    assert out_csv.exists() and out_csv.stat().st_size > 0
