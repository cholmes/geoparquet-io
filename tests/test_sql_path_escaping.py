"""Regression tests: a file path is escaped for SQL exactly once.

``file_utils.safe_file_url()`` escapes a path for interpolation into SQL
(``'`` -> ``''``) *and* validates that the raw path exists.
``duckdb_metadata._safe_url()`` delegates to it, so every public
``duckdb_metadata`` getter escapes its own ``parquet_file`` argument.

Callers that passed their already-escaped ``safe_url`` into those getters
therefore escaped twice: the getter re-escaped ``o''brien`` to ``o''''brien``,
and ``safe_file_url``'s existence check then failed on the *escaped* string,
raising ``FileNotFoundGeoParquetError`` for a file that is plainly there.

The contract is now: ``safe_file_url``/``_safe_url`` is the single escape
point, and every ``duckdb_metadata`` getter takes a RAW path.
"""

from __future__ import annotations

import json

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add, check, convert, inspect
from geoparquet_io.core.duckdb_metadata import (
    detect_geometry_columns,
    get_bbox_from_row_group_stats,
    get_column_names,
    get_compression_info,
    get_file_metadata,
    get_geo_metadata,
    get_per_row_group_bbox_stats,
    get_schema_info,
    has_bbox_column,
)
from geoparquet_io.core.exceptions import FileNotFoundGeoParquetError
from geoparquet_io.core.file_utils import safe_file_url


@pytest.fixture
def apostrophe_file(tmp_path):
    """A GeoParquet file inside a directory whose name contains an apostrophe."""
    directory = tmp_path / "o'brien"
    directory.mkdir()
    path = str(directory / "q.parquet")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    values = ", ".join(
        f"({i + 1}, ST_AsWKB(ST_GeomFromText('POINT ({i} {i})')))" for i in range(10)
    )
    table = con.execute(f"SELECT * FROM (VALUES {values}) AS t(id, geometry)").arrow().read_all()
    con.close()

    geo = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "bbox": [0.0, 0.0, 9.0, 9.0],
            }
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)
    return path


@pytest.fixture
def apostrophe_file_with_bbox(apostrophe_file, tmp_path):
    """The same file, with a bbox covering column added."""
    out = str(tmp_path / "o'brien" / "q_bbox.parquet")
    runner = CliRunner()
    result = runner.invoke(add, ["bbox", apostrophe_file, out])
    assert result.exit_code == 0, result.output
    return out


class TestSafeFileUrlIsIdempotentlyApplied:
    """Passing an already-escaped path to a duckdb_metadata getter must not
    re-escape it. Before the fix these raised FileNotFoundGeoParquetError."""

    def test_escaping_twice_is_fatal(self, apostrophe_file):
        """The premise of this module: escaping is not idempotent."""
        once = safe_file_url(apostrophe_file)
        assert "o''brien" in once
        # Re-escaping mangles the path, and safe_file_url's existence check
        # then fails on a file that is plainly there.
        with pytest.raises(FileNotFoundGeoParquetError):
            safe_file_url(once)

    @pytest.mark.parametrize(
        "getter",
        [
            get_file_metadata,
            get_schema_info,
            get_geo_metadata,
            get_column_names,
            detect_geometry_columns,
            has_bbox_column,
        ],
    )
    def test_metadata_getters_accept_raw_path(self, apostrophe_file, getter):
        # No exception, and something truthy comes back.
        assert getter(apostrophe_file) is not None

    def test_compression_info_accepts_raw_path(self, apostrophe_file):
        assert get_compression_info(apostrophe_file)

    def test_bbox_stats_accept_raw_path(self, apostrophe_file_with_bbox):
        has_bbox, bbox_col = has_bbox_column(apostrophe_file_with_bbox)
        assert has_bbox and bbox_col == "bbox"
        assert get_per_row_group_bbox_stats(apostrophe_file_with_bbox, bbox_col)
        assert get_bbox_from_row_group_stats(apostrophe_file_with_bbox, bbox_col)


class TestCliCommandsOnApostrophePath:
    """End-to-end: every command reported broken by the audit must succeed on a
    path containing an apostrophe."""

    @pytest.mark.parametrize(
        ("group", "args"),
        [
            ("check", ["spatial"]),
            ("check", ["all"]),
            ("check", ["bbox"]),
            ("check", ["spec"]),
            ("check", ["compression"]),
            ("check", ["row-group"]),
            ("inspect", ["meta"]),
            ("inspect", ["summary"]),
            ("inspect", ["head"]),
            ("inspect", ["stats"]),
        ],
    )
    def test_read_only_command_succeeds(self, apostrophe_file, group, args):
        runner = CliRunner()
        cli_group = {"check": check, "inspect": inspect}[group]
        result = runner.invoke(cli_group, [*args, apostrophe_file])

        assert result.exit_code == 0, result.output
        assert "o''brien" not in result.output

    def test_add_bbox_succeeds(self, apostrophe_file, tmp_path):
        out = str(tmp_path / "o'brien" / "out.parquet")
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", apostrophe_file, out])

        assert result.exit_code == 0, result.output
        assert "bbox" in pq.read_schema(out).names

    @pytest.mark.parametrize("fmt", ["geojson", "csv"])
    def test_convert_succeeds(self, apostrophe_file, tmp_path, fmt):
        out = str(tmp_path / "o'brien" / f"out.{fmt}")
        runner = CliRunner()
        result = runner.invoke(convert, [fmt, apostrophe_file, out])

        assert result.exit_code == 0, result.output
        with open(out) as fh:
            assert fh.read().strip()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
