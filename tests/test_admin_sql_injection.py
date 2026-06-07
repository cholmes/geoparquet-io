"""Regression guard for the PR #461 SQL identifier-quoting regression (todo 008).

A geometry/bbox column name is read verbatim from an input file's GeoParquet
metadata (`geo.primary_column`). If that name is interpolated into SQL without
quoting, an attacker-supplied file can inject SQL into the user's DuckDB session,
and any legitimate file with a space/quote in its geometry column name crashes
with a ParserException.

These tests feed `add admin-divisions` / `add country-codes` a file whose
primary geometry column name contains a space, a double-quote, a paren and a
comma, and assert the commands run without a parser error and that the name is
safely quoted (embedded `"` doubled) in the emitted SQL.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add

# A column name that breaks out of an unquoted ST_Intersects(...) / ST_XMin(...)
# call if it is not properly quoted.
MALICIOUS_GEOM_NAME = 'geom") AS pwn, (SELECT 1) AS x, ST_GeomFromText("'


@pytest.fixture
def malicious_geom_column_file(buildings_test_file, tmp_path):
    """A valid GeoParquet whose primary geometry column has a dangerous name.

    Built from buildings_test.parquet (WKB geometry, no bbox column) by renaming
    the geometry column and rewriting the `geo` metadata to point at the new name.
    """
    table = pq.read_table(buildings_test_file)
    new_names = [MALICIOUS_GEOM_NAME if n == "geometry" else n for n in table.column_names]
    table = table.rename_columns(new_names)

    geo = {
        "version": "1.0.0",
        "primary_column": MALICIOUS_GEOM_NAME,
        "columns": {MALICIOUS_GEOM_NAME: {"encoding": "WKB", "geometry_types": ["Polygon"]}},
    }
    existing = table.schema.metadata or {}
    md = {k: v for k, v in existing.items() if k != b"geo"}
    md[b"geo"] = json.dumps(geo).encode()
    table = table.replace_schema_metadata(md)

    out = str(tmp_path / "malicious_geom.parquet")
    pq.write_table(table, out)
    return out


def test_admin_divisions_quotes_malicious_geom_column(malicious_geom_column_file):
    """admin-divisions does not crash / inject on a hostile geometry column name."""
    runner = CliRunner()
    result = runner.invoke(
        add,
        [
            "admin-divisions",
            malicious_geom_column_file,
            "output.parquet",
            "--dataset",
            "gaul",
            "--levels",
            "continent",
            "--dry-run",
            "--no-cache",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "DRY RUN MODE" in result.output
    # The embedded double-quote must be doubled inside a quoted identifier; the
    # raw `AS pwn` breakout must never appear as executable SQL.
    assert 'geom"") AS pwn' in result.output


def test_country_codes_join_query_quotes_identifiers():
    """country_codes spatial-join builder quotes hostile geom/bbox column names.

    The country_codes module is exercised by benchmarks/legacy paths rather than
    a CLI command, so we guard its query builder directly. Both the input geometry
    column (from `geo.primary_column`) and a `--countries-parquet` geometry column
    are untrusted; neither may break out of the generated SQL.
    """
    from geoparquet_io.core.add.country_codes import _build_spatial_join_query

    query = _build_spatial_join_query(
        input_url="input.parquet",
        countries_source="'countries.parquet'",
        select_clause="b.iso_a2 as country",
        input_geom_col=MALICIOUS_GEOM_NAME,
        countries_geom_col="geometry",
        input_bbox_col='bb") AS pwn, (',
        countries_bbox_col="geometry_bbox",
    )

    # Embedded double-quotes are doubled inside quoted identifiers; the raw
    # breakout text must never appear unquoted/executable.
    assert 'geom"") AS pwn' in query
    assert 'bb"") AS pwn' in query
    # ST_Intersects references the quoted geometry identifiers, not bare names.
    assert 'a."' + MALICIOUS_GEOM_NAME.replace('"', '""') + '"' in query
