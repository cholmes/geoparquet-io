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

Issue #718 found three more paths of the same family:

* ``convert geoparquet`` handed its already-escaped ``input_url`` to
  ``crs_utils.extract_crs_from_parquet``, which escapes its own argument
  (double escape -> ``FileNotFoundGeoParquetError``);
* ``add geometry-metrics --vecorel`` reached
  ``constants.ensure_vecorel_columns``, which pre-escaped a path before handing
  it to ``get_column_names`` -- which escapes it again, then hands it to
  pyarrow;
* ``add admin-divisions`` / ``add country-codes`` interpolated the RAW,
  *unescaped* ``output_parquet`` CLI argument straight into ``FROM '{...}'``,
  producing a ``ParserException`` *after* the output file had been written.

The structural fix is :func:`geoparquet_io.core.duckdb_utils.sql_path`, which
turns a RAW path into a complete, quoted SQL literal so call sites stop writing
the quotes -- and the escape -- by hand.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

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
from geoparquet_io.core.duckdb_utils import get_duckdb_connection, sql_path
from geoparquet_io.core.exceptions import FileNotFoundGeoParquetError
from geoparquet_io.core.file_utils import safe_file_url

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_DATA_DIR = Path(__file__).parent / "data"


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


class TestSqlPath:
    """``sql_path`` is the one place a file path becomes a SQL literal."""

    def test_wraps_and_escapes_in_one_step(self):
        assert sql_path("/tmp/it's_data.parquet") == "'/tmp/it''s_data.parquet'"

    def test_plain_path_is_merely_quoted(self):
        assert sql_path("/tmp/data.parquet") == "'/tmp/data.parquet'"

    def test_result_is_a_usable_duckdb_literal(self, apostrophe_file):
        con = get_duckdb_connection(load_spatial=False)
        try:
            rows = con.execute(f"SELECT count(*) FROM {sql_path(apostrophe_file)}").fetchone()
        finally:
            con.close()
        assert rows[0] == 10

    def test_takes_a_raw_path_not_a_safe_file_url_result(self, apostrophe_file):
        """The contract is RAW in.

        No heuristic guard is possible -- ``a''b.parquet`` is a legal filename --
        so the guarantee is structural: ``sql_path`` is the only escape, and a
        caller never reaches for ``safe_file_url`` as well.
        """
        assert sql_path(apostrophe_file) == f"'{safe_file_url(apostrophe_file)}'"


class TestApostropheInInputPath:
    """Commands that double-escaped an already-safe input URL (issue #718)."""

    def test_convert_geoparquet_succeeds(self, apostrophe_file, tmp_path):
        """Was: ``Error: Conversion failed: File not found: .../o''brien/q.parquet``."""
        out = str(tmp_path / "converted.parquet")
        runner = CliRunner()
        result = runner.invoke(convert, ["geoparquet", apostrophe_file, out])

        assert result.exit_code == 0, result.output
        assert "o''brien" not in result.output
        assert pq.read_metadata(out).num_rows == 10

    def test_convert_reproject_succeeds(self, apostrophe_file, tmp_path):
        """Was: ``Error: Cannot read file: .../o''brien/q.parquet``.

        ``reproject._detect_source_crs`` / ``_get_bbox_column_name`` were handed
        the escaped URL, and both delegate to helpers that escape their own
        argument.
        """
        out = str(tmp_path / "reprojected.parquet")
        runner = CliRunner()
        result = runner.invoke(
            convert, ["reproject", apostrophe_file, out, "--dst-crs", "EPSG:3857"]
        )

        assert result.exit_code == 0, result.output
        assert "o''brien" not in result.output
        assert pq.read_metadata(out).num_rows == 10

    def test_add_bbox_metadata_succeeds(self, apostrophe_file_with_bbox):
        """Was: ``Error: Cannot read schema: .../q_bbox''.parquet``.

        ``add bbox-metadata`` rewrites in place, so the apostrophe is in the one
        path it touches.
        """
        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", apostrophe_file_with_bbox])

        assert result.exit_code == 0, result.output
        assert "''" not in result.output
        geo = json.loads(pq.read_schema(apostrophe_file_with_bbox).metadata[b"geo"])
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_extract_crs_from_parquet_takes_a_raw_path(self, apostrophe_file):
        """The helper's contract is RAW in, like every duckdb_metadata getter."""
        from geoparquet_io.core.crs_utils import extract_crs_from_parquet

        # No exception: the file is plainly there, whatever the CRS turns out to be.
        extract_crs_from_parquet(apostrophe_file)

    @pytest.mark.parametrize(
        ("fixture", "extra_args"),
        [
            # The plain read path...
            ("buildings_test.gpkg", []),
            # ...and the linearized one, which re-reads the source separately.
            ("curved_geometry_test.gpkg", ["--linearize-curves"]),
        ],
    )
    def test_convert_geoparquet_from_gpkg_with_apostrophe(self, tmp_path, fixture, extra_args):
        """Was: ``Error: No CRS found in input file: .../o''brien.gpkg``.

        A GeoPackage takes the *spatial* branch of the CRS detection, which
        double-escaped the path: ``detect_crs_from_spatial_file`` escapes its own
        argument, so ST_Read looked for ``o''brien.gpkg`` and found nothing --
        and the empty result was reported as a missing CRS, not a missing file.
        """
        source = TEST_DATA_DIR / fixture
        gpkg = tmp_path / "o'brien.gpkg"
        gpkg.write_bytes(source.read_bytes())
        out = str(tmp_path / "out.parquet")

        runner = CliRunner()
        result = runner.invoke(convert, ["geoparquet", str(gpkg), out, *extra_args])

        assert result.exit_code == 0, result.output
        assert "o''brien" not in result.output
        assert pq.read_metadata(out).num_rows > 0

    def test_get_row_group_geo_stats_takes_a_raw_path(self, tmp_path):
        """Was: ``FileNotFoundGeoParquetError`` on ``.../o''brien.parquet``.

        The public API escaped the path itself and then handed the escaped
        string to getters that escape their own argument.
        """
        from geoparquet_io.api import ops

        source = TEST_DATA_DIR / "fields_pgo_crs84_bbox_snappy.parquet"
        path = tmp_path / "o'brien.parquet"
        path.write_bytes(source.read_bytes())

        stats = ops.get_row_group_geo_stats(str(path))

        assert stats
        assert {"row_group_id", "xmin", "ymin", "xmax", "ymax"} <= set(stats[0])


class TestApostropheInOutputPath:
    """Commands that mangled or failed to escape the output path (issue #718)."""

    def test_add_geometry_metrics_succeeds(self, apostrophe_file, tmp_path):
        """Was: ``Error: Cannot read schema: .../o'brien/out''_gm.parquet``."""
        out = str(tmp_path / "o'brien" / "out'_gm.parquet")
        runner = CliRunner()
        result = runner.invoke(add, ["geometry-metrics", apostrophe_file, out])

        assert result.exit_code == 0, result.output
        assert "''" not in result.output
        assert "metrics:area" in pq.read_schema(out).names

    def test_get_result_stats_quotes_the_output_path(self, tmp_path):
        """Was: ``ParserException: syntax error at or near "."`` on ``FROM 'adm'_out.parquet'``.

        Exercised directly: the CLI path needs a downloaded admin dataset, but
        the unescaped interpolation is entirely local to this helper.
        """
        from geoparquet_io.core.add.admin_divisions import _get_result_stats

        class _StubDataset:
            def get_output_column_name(self, level, prefix=None):
                return level

        out = str(tmp_path / "adm'_out.parquet")
        con = get_duckdb_connection(load_spatial=False)
        try:
            con.execute(
                "COPY (SELECT * FROM (VALUES (1, 'Europe'), (2, NULL)) AS t(id, continent)) "
                f"TO {sql_path(out)} (FORMAT PARQUET)"
            )
            total, with_admin, uniques = _get_result_stats(
                con, out, _StubDataset(), ["continent"], False
            )
        finally:
            con.close()

        assert total == 2
        assert with_admin == 1
        assert uniques == [("continent", 1)]

    def test_admin_divisions_dry_run_quotes_the_output_path(self, apostrophe_file, tmp_path):
        """The printed ``COPY ... TO '...'`` must be valid SQL too."""
        out = str(tmp_path / "adm'_out.parquet")
        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "admin-divisions",
                apostrophe_file,
                out,
                "--dataset",
                "gaul",
                "--levels",
                "continent",
                "--dry-run",
                "--no-cache",
            ],
        )

        assert result.exit_code == 0, result.output
        assert f"TO {sql_path(out)}" in result.output


class TestSqlPathLiteralRatchet:
    """The pre-commit ratchet that stops new hand-written ``FROM '{path}'``.

    54 of the existing sites interpolate an already-escaped ``safe_file_url``
    result and are correct, so a flat ban would demand an unreviewable rewrite.
    The ratchet records each file's count and fails only when one goes up.
    """

    @staticmethod
    def _checker():
        return REPO_ROOT / "scripts" / "check_sql_path_literals.py"

    def test_checker_is_wired_into_the_precommit_hook(self):
        """The hook's ``-f`` guard means a missing script would silently pass."""
        assert self._checker().exists()
        config = (REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
        assert "scripts/check_sql_path_literals.py" in config

    def test_current_tree_is_at_or_below_its_baseline(self):
        result = subprocess.run(
            [sys.executable, str(self._checker())],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.parametrize(
        "source",
        [
            "q = f\"SELECT * FROM '{path}'\"\n",
            "q = f\"COPY (SELECT 1) TO '{out}' (FORMAT PARQUET)\"\n",
            "q = f\"SELECT * FROM read_parquet('{url}')\"\n",
        ],
    )
    def test_new_hand_written_literal_is_rejected(self, source):
        spec = importlib.util.spec_from_file_location("_sql_path_checker", self._checker())
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.PATTERN.search(source)

    @pytest.mark.parametrize(
        "source",
        [
            'q = f"SELECT * FROM {sql_path(path)}"\n',
            "# FROM '{path}' in a comment is prose, not code\n",
            "q = f\"SELECT {col} FROM t WHERE name = '{value}'\"\n",
            # Prose in a docstring is not a call site either -- and counting it
            # would let a reworded docstring pay for a real new violation.
            '"""Never write FROM \'{path}\' by hand; use sql_path()."""\n',
            'def f():\n    """Bad:  FROM \'{path}\'\n\n    Good: FROM {sql_path(path)}\n    """\n',
        ],
    )
    def test_correct_code_is_not_flagged(self, source, tmp_path):
        spec = importlib.util.spec_from_file_location("_sql_path_checker", self._checker())
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        probe = tmp_path / "probe.py"
        probe.write_text(source, encoding="utf-8")
        assert module.count_in(probe) == 0

    def test_docstring_prose_cannot_pay_for_a_new_violation(self, tmp_path):
        """End-to-end: the ratchet must not be satisfiable by rewording prose.

        Counting docstring lines made each file's baseline a *budget* that
        documentation could free up: shorten the docstring that mentions
        ``FROM '{path}'``, spend the slot on a real interpolation, and the
        per-file count is unchanged -- so the checker passes a genuine new
        violation. Only real call sites are counted now.
        """
        checker = tmp_path / "scripts" / "check_sql_path_literals.py"
        checker.parent.mkdir()
        checker.write_text(self._checker().read_text(encoding="utf-8"), encoding="utf-8")
        for root in ("geoparquet_io", "plugins"):
            (tmp_path / root).mkdir()
        module = tmp_path / "geoparquet_io" / "reader.py"

        # A file whose docstring explains the rule by quoting the bad form.
        module.write_text(
            '"""Read a file.\n'
            "\n"
            "Never write FROM '{path}' by hand -- the escape gets forgotten.\n"
            "Prefer FROM {sql_path(path)}, which quotes and escapes in one step.\n"
            '"""\n'
            "\n"
            "\n"
            "def read(con, path):\n"
            "    return con.execute(f'SELECT * FROM {sql_path(path)}')\n",
            encoding="utf-8",
        )

        def run(*args):
            return subprocess.run(
                [sys.executable, str(checker), *args],
                cwd=tmp_path,
                capture_output=True,
                text=True,
                encoding="utf-8",
            )

        assert run("--update").returncode == 0
        assert run().returncode == 0, "the unmodified tree is its own baseline"

        # Reword the docstring down to one line, and spend the freed slot on a
        # real hand-written interpolation.
        module.write_text(
            '"""Read a file."""\n'
            "\n"
            "\n"
            "def read(con, path):\n"
            "    return con.execute(f\"SELECT * FROM '{path}'\")\n",
            encoding="utf-8",
        )

        result = run()
        assert result.returncode != 0, result.stdout + result.stderr
        assert "geoparquet_io/reader.py" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
