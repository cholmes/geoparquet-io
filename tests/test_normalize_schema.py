"""Tests for schema-layout normalization (core, CLI, Python API).

Normalization rewrites a GeoParquet file's schema for the tri-access layout
(spec §1): lowercase column names, deterministic order (attributes, then the
geometry column, then bbox covering columns last), contiguous ``PARQUET:field_id``
on every top-level column, and optional per-column ``description``. Geometry
encoding and CRS are left untouched.
"""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner


def _bbox_struct(n: int) -> pa.Array:
    fields = [pa.field(f, pa.float64()) for f in ("xmin", "ymin", "xmax", "ymax")]
    return pa.array(
        [{"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0}] * n, type=pa.struct(fields)
    )


def _make_table(columns, *, primary, covering=None):
    """Build a table from ordered (name, array) pairs + GeoParquet metadata.

    ``columns``: list of (name, pa.Array). ``primary``: geometry column name.
    ``covering``: bbox covering column name, or None.
    """
    tbl = pa.table(dict(columns), schema=pa.schema([pa.field(n, a.type) for n, a in columns]))
    geo = {
        "version": "1.1.0",
        "primary_column": primary,
        "columns": {primary: {"encoding": "WKB", "geometry_types": []}},
    }
    if covering:
        geo["columns"][primary]["covering"] = {
            "bbox": {f: [covering, f] for f in ("xmin", "ymin", "xmax", "ymax")}
        }
    return tbl.replace_schema_metadata({b"geo": json.dumps(geo).encode()})


def _mixed_table(n: int = 3):
    """Table with mixed-case names, geom first, bbox in the middle."""
    return _make_table(
        [
            ("Geometry", pa.array([b"\x00"] * n, type=pa.binary())),
            ("Name", pa.array([f"r{i}" for i in range(n)])),
            ("Pop", pa.array(list(range(n)))),
            ("bbox", _bbox_struct(n)),
        ],
        primary="Geometry",
        covering="bbox",
    )


def _field_id(field: pa.Field) -> str | None:
    md = field.metadata or {}
    v = md.get(b"PARQUET:field_id")
    return v.decode() if v else None


def _geo_meta(table: pa.Table) -> dict:
    return json.loads(table.schema.metadata[b"geo"].decode())


class TestNormalizeSchemaTable:
    def test_lowercases_names(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        out = normalize_schema_table(_mixed_table())
        assert all(n == n.lower() for n in out.column_names)

    def test_orders_attrs_then_geom_then_bbox(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        out = normalize_schema_table(_mixed_table())
        assert out.column_names == ["name", "pop", "geometry", "bbox"]

    def test_assigns_contiguous_field_ids(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        out = normalize_schema_table(_mixed_table())
        ids = [_field_id(out.schema.field(n)) for n in out.column_names]
        assert ids == ["1", "2", "3", "4"]

    def test_geometry_id_precedes_bbox_with_no_gap(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        # attributes + geom must occupy a contiguous 1..k block (Iceberg requirement);
        # bbox columns come strictly after.
        out = normalize_schema_table(_mixed_table())
        geom_id = int(_field_id(out.schema.field("geometry")))
        bbox_id = int(_field_id(out.schema.field("bbox")))
        assert geom_id == 3 and bbox_id == 4

    def test_updates_primary_column_in_geo_metadata(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        out = normalize_schema_table(_mixed_table())
        assert _geo_meta(out)["primary_column"] == "geometry"

    def test_preserves_data(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        out = normalize_schema_table(_mixed_table())
        assert out.column("name").to_pylist() == ["r0", "r1", "r2"]

    def test_applies_descriptions(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        out = normalize_schema_table(_mixed_table(), descriptions={"name": "the feature name"})
        md = out.schema.field("name").metadata or {}
        assert md.get(b"description") == b"the feature name"

    def test_lowercase_collision_raises(self):
        from geoparquet_io.core.normalize_schema import normalize_schema_table

        t = _make_table(
            [
                ("geometry", pa.array([b"\x00"], type=pa.binary())),
                ("Name", pa.array(["a"])),
                ("name", pa.array(["b"])),
            ],
            primary="geometry",
        )
        with pytest.raises(ValueError, match="(?i)collision|lowercas"):
            normalize_schema_table(t)


class TestNormalizeSchemaCLI:
    def test_cli_writes_field_ids_to_parquet(self, tmp_path):
        from geoparquet_io.cli.main import cli

        src = str(tmp_path / "in.parquet")
        pq.write_table(_mixed_table(), src)
        out = str(tmp_path / "out.parquet")
        result = CliRunner().invoke(cli, ["normalize-schema", src, out])
        assert result.exit_code == 0, result.output

        schema = pq.ParquetFile(out).schema_arrow
        assert schema.names == ["name", "pop", "geometry", "bbox"]
        ids = [(schema.field(n).metadata or {}).get(b"PARQUET:field_id") for n in schema.names]
        assert ids == [b"1", b"2", b"3", b"4"]


class TestNormalizeSchemaPythonAPI:
    def test_ops_function(self):
        from geoparquet_io.api import ops

        out = ops.normalize_schema(_mixed_table())
        assert out.column_names == ["name", "pop", "geometry", "bbox"]

    def test_table_method_chainable(self):
        from geoparquet_io.api.table import Table

        result = Table(_mixed_table()).normalize_schema()
        assert isinstance(result, Table)
        assert result.column_names == ["name", "pop", "geometry", "bbox"]
        # Geometry column was "Geometry"; re-detected as lowercased "geometry".
        assert result.geometry_column == "geometry"
