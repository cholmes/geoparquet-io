"""Tests for gpio-fiboa plugin."""

import json
import os
import tempfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from gpio_fiboa.cli import fiboa
from gpio_fiboa.validate import validate_fiboa

TEST_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "tests" / "data"
BUILDINGS_TEST_FILE = TEST_DATA_DIR / "buildings_test.parquet"


@pytest.fixture
def buildings_file():
    """Path to buildings test file with polygon geometries."""
    return str(BUILDINGS_TEST_FILE)


@pytest.fixture
def temp_output():
    """Temporary output file path."""
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    os.unlink(path)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def metrics_file(buildings_file, temp_output):
    """Buildings file with geometry metrics already added."""
    from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

    add_geometry_metrics(buildings_file, temp_output, vecorel=True)
    return temp_output


class TestFiboaValidate:
    """Test gpio fiboa validate command."""

    def test_validate_plain_file(self, buildings_file):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["validate", buildings_file])
        # Plain file should pass but with warnings about missing metadata
        assert "Warning" in result.output or "Valid" in result.output

    def test_validate_metrics_file(self, metrics_file):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["validate", metrics_file])
        assert result.exit_code == 0

    def test_validate_core_function(self, buildings_file):
        is_valid = validate_fiboa(buildings_file)
        assert isinstance(is_valid, bool)


class TestFiboaDescribe:
    """Test gpio fiboa describe command."""

    def test_describe_plain_file(self, buildings_file):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["describe", buildings_file])
        assert result.exit_code == 0
        assert "fiboa description" in result.output
        assert "geometry" in result.output

    def test_describe_metrics_file(self, metrics_file):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["describe", metrics_file])
        assert result.exit_code == 0
        assert "metrics:area" in result.output
        assert "metrics:perimeter" in result.output
        assert "Geometry Metrics" in result.output

    def test_describe_verbose(self, buildings_file):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["describe", buildings_file, "-v"])
        assert result.exit_code == 0


class TestFiboaImprove:
    """Test gpio fiboa improve command."""

    def test_improve_with_metrics(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["improve", buildings_file, temp_output, "-sz"])
        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output)

        pf = pq.ParquetFile(temp_output)
        col_names = pf.schema_arrow.names
        assert "metrics:area" in col_names
        assert "metrics:perimeter" in col_names

    def test_improve_with_schemas(self, metrics_file):
        fd, output = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(output)
        try:
            runner = CliRunner()
            result = runner.invoke(fiboa, ["improve", metrics_file, output, "-s"])
            assert result.exit_code == 0, result.output

            pf = pq.ParquetFile(output)
            meta = pf.schema_arrow.metadata
            assert b"collection" in meta
            vecorel = json.loads(meta[b"collection"])
            schemas = vecorel["schemas"]["default"]
            assert "https://fiboa.org/specification/v0.3.0/schema.yaml" in schemas
        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_improve_no_flags_warns(self):
        runner = CliRunner()
        result = runner.invoke(
            fiboa, ["improve", "input.parquet", "output.parquet", "--skip-hilbert"]
        )
        assert result.exit_code == 0
        assert "No improvements requested" in result.output

    def test_improve_with_geoparquet_version(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            ["improve", buildings_file, temp_output, "-sz", "--geoparquet-version", "1.1"],
        )
        assert result.exit_code == 0, result.output

        pf = pq.ParquetFile(temp_output)
        meta = pf.schema_arrow.metadata
        geo = json.loads(meta[b"geo"])
        assert geo["version"] == "1.1.0"

    def test_improve_with_row_group_size(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            ["improve", buildings_file, temp_output, "-sz", "--row-group-size", "10"],
        )
        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output)

    def test_improve_with_compression_level(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            [
                "improve",
                buildings_file,
                temp_output,
                "-sz",
                "--compression",
                "ZSTD",
                "--compression-level",
                "22",
            ],
        )
        assert result.exit_code == 0, result.output
        assert os.path.exists(temp_output)

    def test_improve_help_shows_all_options(self):
        runner = CliRunner()
        result = runner.invoke(fiboa, ["improve", "--help"])
        assert "--geoparquet-version" in result.output
        assert "--row-group-size" in result.output
        assert "--row-group-size-mb" in result.output
        assert "--compression-level" in result.output
        assert "--compression" in result.output

    def test_improve_with_determination_method(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            [
                "improve",
                buildings_file,
                temp_output,
                "-sz",
                "--determination-method",
                "auto-imagery",
            ],
        )
        assert result.exit_code == 0, result.output

        import duckdb

        with duckdb.connect() as con:
            con.execute("LOAD spatial;")
            vals = con.execute(
                f"""SELECT DISTINCT "determination:method" FROM read_parquet('{temp_output}')"""
            ).fetchall()
            assert vals == [("auto-imagery",)]

    def test_improve_datetime_column_removes_source(self, buildings_file, temp_output):
        """When mapping a column, the source column should be removed by default."""
        import duckdb

        fd, with_time = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(with_time)
        try:
            with duckdb.connect() as con:
                con.execute("LOAD spatial;")
                con.execute(
                    f"COPY (SELECT *, TIMESTAMP '2024-01-01' AS time FROM "
                    f"'{os.path.normpath(buildings_file)}') "
                    f"TO '{with_time}' (FORMAT PARQUET)"
                )

            runner = CliRunner()
            result = runner.invoke(
                fiboa,
                ["improve", with_time, temp_output, "--determination-datetime", "time"],
            )
            assert result.exit_code == 0, result.output

            with duckdb.connect() as con:
                con.execute("LOAD spatial;")
                cols = [
                    c[0]
                    for c in con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{temp_output}')"
                    ).fetchall()
                ]
                assert "determination:datetime" in cols
                assert "time" not in cols
        finally:
            if os.path.exists(with_time):
                os.unlink(with_time)

    def test_improve_datetime_keep_source_columns(self, buildings_file, temp_output):
        """--keep-source-columns should preserve the original column."""
        import duckdb

        fd, with_time = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(with_time)
        try:
            with duckdb.connect() as con:
                con.execute("LOAD spatial;")
                con.execute(
                    f"COPY (SELECT *, TIMESTAMP '2024-01-01' AS time FROM "
                    f"'{os.path.normpath(buildings_file)}') "
                    f"TO '{with_time}' (FORMAT PARQUET)"
                )

            runner = CliRunner()
            result = runner.invoke(
                fiboa,
                [
                    "improve",
                    with_time,
                    temp_output,
                    "--determination-datetime",
                    "time",
                    "--keep-source-columns",
                ],
            )
            assert result.exit_code == 0, result.output

            with duckdb.connect() as con:
                con.execute("LOAD spatial;")
                cols = [
                    c[0]
                    for c in con.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{temp_output}')"
                    ).fetchall()
                ]
                assert "determination:datetime" in cols
                assert "time" in cols
        finally:
            if os.path.exists(with_time):
                os.unlink(with_time)

    def test_improve_with_determination_datetime_literal(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            [
                "improve",
                buildings_file,
                temp_output,
                "-sz",
                "--determination-datetime",
                "2024-01-01T00:00:00Z",
            ],
        )
        assert result.exit_code == 0, result.output

        import duckdb

        with duckdb.connect() as con:
            con.execute("LOAD spatial;")
            cols = [
                c[0]
                for c in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{temp_output}')"
                ).fetchall()
            ]
            assert "determination:datetime" in cols

    def test_improve_with_category(self, buildings_file, temp_output):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            ["improve", buildings_file, temp_output, "-sz", "--category", "operational,economic"],
        )
        assert result.exit_code == 0, result.output

        import duckdb

        with duckdb.connect() as con:
            con.execute("LOAD spatial;")
            cols = [
                c[0]
                for c in con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{temp_output}')"
                ).fetchall()
            ]
            assert "category" in cols

    def test_improve_invalid_determination_method(self):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            [
                "improve",
                "input.parquet",
                "output.parquet",
                "--determination-method",
                "invalid-method",
            ],
        )
        assert result.exit_code != 0

    def test_improve_invalid_category(self):
        runner = CliRunner()
        result = runner.invoke(
            fiboa,
            ["improve", "input.parquet", "output.parquet", "--category", "not-a-category"],
        )
        assert result.exit_code != 0

    def test_improve_auto_downgrades_geoparquet_v2(self, buildings_file, temp_output):
        """Native geo type input should auto-downgrade to 1.1 for vecorel compatibility."""
        import duckdb as _duckdb

        fd, v2_file = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        try:
            with _duckdb.connect() as con:
                con.execute("LOAD spatial;")
                con.execute(
                    f"COPY (SELECT * FROM '{buildings_file}' LIMIT 10) "
                    f"TO '{v2_file}' (FORMAT PARQUET, GEOPARQUET_VERSION 'NONE')"
                )

            runner = CliRunner()
            result = runner.invoke(fiboa, ["improve", v2_file, temp_output, "-sz", "-s"])
            assert result.exit_code == 0, result.output

            pf = pq.ParquetFile(temp_output)
            meta = pf.schema_arrow.metadata
            geo = json.loads(meta[b"geo"])
            assert geo["version"] == "1.1.0"

            col_names = pf.schema_arrow.names
            assert "bbox" in col_names
        finally:
            if os.path.exists(v2_file):
                os.unlink(v2_file)

    def test_improve_metrics_and_schemas(self, buildings_file):
        fd, output = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(output)
        try:
            runner = CliRunner()
            result = runner.invoke(fiboa, ["improve", buildings_file, output, "-sz", "-s"])
            assert result.exit_code == 0, result.output

            pf = pq.ParquetFile(output)
            col_names = pf.schema_arrow.names
            assert "metrics:area" in col_names

            meta = pf.schema_arrow.metadata
            vecorel = json.loads(meta[b"collection"])
            schemas = vecorel["schemas"]["default"]
            assert "https://fiboa.org/specification/v0.3.0/schema.yaml" in schemas
            assert "https://vecorel.org/geometry-metrics-extension/v0.1.0/schema.yaml" in schemas
        finally:
            if os.path.exists(output):
                os.unlink(output)

    def test_improve_admin_adds_bbox_before_join(self, buildings_file, temp_output, monkeypatch):
        """With -a, a bbox column is added before the admin spatial join.

        The admin join's bbox pre-filter needs an input bbox column; without it
        the join falls back to slow full-geometry intersection. The real Overture
        join needs network, so we stub it and assert its input already has a bbox.
        """
        import shutil

        from geoparquet_io.core.common import check_bbox_structure

        # Premise: input has no bbox column.
        assert check_bbox_structure(buildings_file, verbose=False)["has_bbox_column"] is False

        seen = {}

        def fake_admin(input_parquet, output_parquet, **kwargs):
            seen["input_had_bbox"] = check_bbox_structure(input_parquet, verbose=False)[
                "has_bbox_column"
            ]
            shutil.copy(input_parquet, output_parquet)

        monkeypatch.setattr(
            "geoparquet_io.core.add.admin_divisions.add_admin_divisions_multi",
            fake_admin,
        )

        runner = CliRunner()
        result = runner.invoke(
            fiboa, ["improve", buildings_file, temp_output, "-a", "--skip-hilbert"]
        )
        assert result.exit_code == 0, result.output

        # The admin join received an input that already had a bbox column.
        assert seen.get("input_had_bbox") is True
        # And the final output keeps it.
        assert "bbox" in pq.ParquetFile(temp_output).schema_arrow.names

    def test_improve_admin_uses_vecorel_column_names(
        self, buildings_file, temp_output, monkeypatch
    ):
        """`-a` must request vecorel naming so columns are admin:country_code /
        admin:subdivision_code (the fiboa spec names), not overture_*."""
        import shutil

        seen = {}

        def fake_admin(input_parquet, output_parquet, **kwargs):
            seen["vecorel"] = kwargs.get("vecorel")
            shutil.copy(input_parquet, output_parquet)

        monkeypatch.setattr(
            "geoparquet_io.core.add.admin_divisions.add_admin_divisions_multi",
            fake_admin,
        )

        runner = CliRunner()
        result = runner.invoke(
            fiboa, ["improve", buildings_file, temp_output, "-a", "--skip-hilbert"]
        )
        assert result.exit_code == 0, result.output
        assert seen.get("vecorel") is True
