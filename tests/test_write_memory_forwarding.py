"""Tests asserting --write-memory is forwarded (not silently dropped).

Companion to the sort-hilbert fix in PR #627: the `output_format_options`
decorator attaches `--write-memory` to every command, but several `add`/`sort`
commands captured `write_memory` in their signature and never forwarded it to
the core function / DuckDB write engine. This file locks down forwarding for:

    add h3, add a5, add s2, add kdtree, add quadkey, sort column, sort quadkey

Three layers are tested:
  * CLI -> core function: the CLI passes ``write_memory`` through as the
    keyword argument ``memory_limit`` (not positionally, not dropped).
  * Core function (file-based path) -> DuckDB write engine: a full,
    unmocked invocation shows "DuckDB memory limit: <value>" in --verbose
    output, mirroring the existing sort-hilbert regression test.
  * Core function (streaming path) -> execute_transform/write_output: the
    private streaming helpers forward memory_limit too.
"""

from __future__ import annotations

from unittest import mock

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def _find_non_geometry_column(parquet_file: str) -> str:
    conn = duckdb.connect()
    try:
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{parquet_file}"').fetchall()
    finally:
        conn.close()
    for col in columns:
        if col[0] != "geometry":
            return col[0]
    raise AssertionError("No non-geometry columns found in test fixture")


# ---------------------------------------------------------------------------
# Layer 1: CLI forwards --write-memory as memory_limit=<value> BY KEYWORD
# ---------------------------------------------------------------------------

CLI_FORWARDING_CASES = [
    pytest.param(
        "geoparquet_io.cli.main.add_h3_column_impl",
        lambda in_f, out_f, col: ["add", "h3", in_f, out_f],
        id="add-h3",
    ),
    pytest.param(
        "geoparquet_io.cli.main.add_a5_column_impl",
        lambda in_f, out_f, col: ["add", "a5", in_f, out_f],
        id="add-a5",
    ),
    pytest.param(
        "geoparquet_io.cli.main.add_s2_column_impl",
        lambda in_f, out_f, col: ["add", "s2", in_f, out_f],
        id="add-s2",
    ),
    pytest.param(
        "geoparquet_io.cli.main.add_kdtree_column_impl",
        lambda in_f, out_f, col: ["add", "kdtree", in_f, out_f],
        id="add-kdtree",
    ),
    pytest.param(
        "geoparquet_io.cli.main.add_quadkey_column_impl",
        lambda in_f, out_f, col: ["add", "quadkey", in_f, out_f],
        id="add-quadkey",
    ),
    pytest.param(
        "geoparquet_io.cli.main.sort_by_column_impl",
        lambda in_f, out_f, col: ["sort", "column", in_f, out_f, col],
        id="sort-column",
    ),
    pytest.param(
        "geoparquet_io.cli.main.sort_by_quadkey_impl",
        lambda in_f, out_f, col: ["sort", "quadkey", in_f, out_f],
        id="sort-quadkey",
    ),
]


class TestCliForwardsWriteMemoryByKeyword:
    """The CLI must call the core function with memory_limit=<value> as a kwarg."""

    @pytest.mark.parametrize("impl_target,build_args", CLI_FORWARDING_CASES)
    def test_write_memory_forwarded_as_keyword(self, impl_target, build_args):
        runner = CliRunner()
        args = build_args("input.parquet", "output.parquet", "some_column")
        args = [*args, "--write-memory", "222MB"]

        with mock.patch(impl_target) as mocked_impl:
            result = runner.invoke(cli, args)

        assert result.exit_code == 0, result.output
        mocked_impl.assert_called_once()
        _call_args, call_kwargs = mocked_impl.call_args
        assert "memory_limit" in call_kwargs, (
            f"{impl_target} was not called with memory_limit as a keyword argument "
            f"(kwargs were: {sorted(call_kwargs)})"
        )
        assert call_kwargs["memory_limit"] == "222MB"


# ---------------------------------------------------------------------------
# Layer 2: real (unmocked) file-based invocation reaches the DuckDB write
# engine, mirroring tests/test_sort.py::test_hilbert_sort_write_memory_reaches_engine
# ---------------------------------------------------------------------------


class TestFileBasedWriteMemoryReachesEngine:
    def test_add_h3_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "h3",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "513MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 513MB" in result.output

    def test_add_a5_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "a5",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "514MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 514MB" in result.output

    def test_add_s2_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "s2",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "515MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 515MB" in result.output

    def test_add_kdtree_write_memory_reaches_engine(self, buildings_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "kdtree",
                buildings_test_file,
                temp_output_file,
                "--write-memory",
                "516MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 516MB" in result.output

    def test_add_quadkey_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "quadkey",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "517MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 517MB" in result.output

    def test_sort_column_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        test_column = _find_non_geometry_column(places_test_file)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "column",
                places_test_file,
                temp_output_file,
                test_column,
                "--write-memory",
                "518MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 518MB" in result.output

    def test_sort_quadkey_write_memory_reaches_engine(self, places_test_file, temp_output_file):
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "sort",
                "quadkey",
                places_test_file,
                temp_output_file,
                "--write-memory",
                "519MB",
                "--verbose",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "DuckDB memory limit: 519MB" in result.output


# ---------------------------------------------------------------------------
# Layer 3: streaming helpers forward memory_limit into execute_transform /
# write_output.
# ---------------------------------------------------------------------------


class TestStreamingHelpersForwardMemoryLimit:
    def test_add_h3_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.add.h3 import _add_h3_streaming

        with mock.patch("geoparquet_io.core.add.h3.execute_transform") as mocked:
            _add_h3_streaming(
                "in.parquet",
                "out.parquet",
                "h3_cell",
                9,
                False,
                "ZSTD",
                None,
                None,
                None,
                None,
                None,
                memory_limit="601MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "601MB"

    def test_add_a5_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.add.a5 import _add_a5_streaming

        with mock.patch("geoparquet_io.core.add.a5.execute_transform") as mocked:
            _add_a5_streaming(
                "in.parquet",
                "out.parquet",
                "a5_cell",
                15,
                False,
                "ZSTD",
                None,
                None,
                None,
                None,
                None,
                memory_limit="602MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "602MB"

    def test_add_s2_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.add.s2 import _add_s2_streaming

        with mock.patch("geoparquet_io.core.add.s2.execute_transform") as mocked:
            _add_s2_streaming(
                "in.parquet",
                "out.parquet",
                "s2_cell",
                13,
                False,
                "ZSTD",
                None,
                None,
                None,
                None,
                None,
                memory_limit="603MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "603MB"

    def test_sort_by_column_streaming_forwards_memory_limit(self):
        from geoparquet_io.core.sort_by_column import _sort_by_column_streaming

        with mock.patch("geoparquet_io.core.sort_by_column.execute_transform") as mocked:
            _sort_by_column_streaming(
                "in.parquet",
                "out.parquet",
                ["name"],
                False,
                False,
                "ZSTD",
                None,
                None,
                None,
                None,
                None,
                memory_limit="604MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "604MB"

    def test_add_kdtree_streaming_forwards_memory_limit(self, buildings_test_file):
        from geoparquet_io.core.add.kdtree import _add_kdtree_streaming

        with mock.patch("geoparquet_io.core.add.kdtree.write_output") as mocked:
            _add_kdtree_streaming(
                buildings_test_file,
                "unused_output.parquet",
                "kdtree_cell",
                1,
                False,
                "ZSTD",
                None,
                None,
                None,
                100,
                None,
                None,
                memory_limit="605MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "605MB"

    def test_add_quadkey_streaming_forwards_memory_limit(self, places_test_file):
        from geoparquet_io.core.add.quadkey import _add_quadkey_streaming

        with mock.patch("geoparquet_io.core.add.quadkey.write_output") as mocked:
            _add_quadkey_streaming(
                places_test_file,
                "unused_output.parquet",
                "quadkey",
                13,
                False,
                False,
                "ZSTD",
                None,
                None,
                None,
                None,
                None,
                memory_limit="606MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "606MB"

    def test_sort_by_quadkey_streaming_forwards_memory_limit(self, places_test_file):
        from geoparquet_io.core.sort_quadkey import _sort_by_quadkey_streaming

        with mock.patch("geoparquet_io.core.sort_quadkey.write_output") as mocked:
            _sort_by_quadkey_streaming(
                places_test_file,
                "unused_output.parquet",
                "quadkey",
                13,
                False,
                False,
                False,
                "ZSTD",
                None,
                None,
                None,
                None,
                None,
                memory_limit="607MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "607MB"

    def test_execute_transform_forwards_memory_limit(self, places_test_file, temp_output_file):
        """execute_transform itself (used by h3/a5/s2/sort column streaming) must
        forward memory_limit into write_output -> write_parquet_with_metadata."""
        from geoparquet_io.core.stream_io import execute_transform

        def make_query(source: str, con) -> str:
            return f"SELECT * FROM {source}"

        with mock.patch("geoparquet_io.core.stream_io.write_output") as mocked:
            execute_transform(
                places_test_file,
                temp_output_file,
                make_query,
                verbose=False,
                memory_limit="608MB",
            )
        mocked.assert_called_once()
        assert mocked.call_args.kwargs.get("memory_limit") == "608MB"
