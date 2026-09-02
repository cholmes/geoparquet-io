"""The 2.0 no-rewrite fast path must not thin out the ``geo`` block.

A 2.0 -> 2.0 write skips the metadata rewrite and lets DuckDB regenerate the
``geo`` key from its own ``GEOPARQUET_VERSION 'V2'`` output. That generated block
carries only ``version``, ``primary_column``, ``encoding``, ``geometry_types``
and ``bbox``, so everything else the input declared was silently dropped:
``epoch``, ``orientation``, and the ``covering`` the rewrite path would have
derived for a conventional ``bbox`` column the output still carries (#772).

The contract pinned here is parity: for the same input, the fast path and the
rewrite path must produce the same ``geo`` block. The rewrite path already makes
that claim, so matching it is consistency rather than a new assertion.
"""

from __future__ import annotations

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

FAST_PATH_MARKER = "using plain COPY TO"
FIBOA_VALUE = '{"fiboa_version": "0.2.0"}'

BASE_GEO = {
    "version": "2.0.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Polygon"],
            "bbox": [18.29540185542694, 47.04283109348567, 18.36084304216915, 47.14564905488368],
            "epoch": 2020.5,
            "orientation": "counterclockwise",
        }
    },
}


def _write_v2_input(
    source: str,
    destination,
    geo: dict,
    bbox_expr: str | None = "conventional",
    extra_kv: dict[str, str] | None = None,
) -> str:
    """Write a native-geometry GeoParquet 2.0 file with a hand-built ``geo`` block.

    ``bbox_expr`` picks the shape of the carried bbox column: the conventional
    ``bbox`` struct, a struct under another name, a non-struct ``bbox``, or none.
    """
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    struct = (
        "{'xmin': ST_XMin(geometry), 'ymin': ST_YMin(geometry), "
        "'xmax': ST_XMax(geometry), 'ymax': ST_YMax(geometry)}"
    )
    projections = {
        "conventional": f"{struct} AS bbox",
        "renamed": f"{struct} AS tile_bounds",
        "not_a_struct": "ST_XMin(geometry) AS bbox",
        None: None,
    }
    bbox_projection = projections[bbox_expr]
    select = "SELECT * EXCLUDE (geometry), geometry"
    if bbox_projection:
        select += f", {bbox_projection}"
    query = f"{select} FROM '{source}'"

    kv = {"geo": json.dumps(geo), **(extra_kv or {})}
    kv_clause = ", ".join(f"{key}: '{value}'" for key, value in kv.items())
    con = get_duckdb_connection()
    try:
        con.execute(
            f"COPY ({query}) TO '{destination}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD, GEOPARQUET_VERSION 'V2', "
            f"KV_METADATA {{ {kv_clause} }})"
        )
    finally:
        con.close()
    return str(destination)


def _geo(path) -> dict:
    return json.loads(pq.ParquetFile(str(path)).schema_arrow.metadata[b"geo"])


def _sort(inp, out) -> str:
    """Drive a 2.0 -> 2.0 write that passes the input's own metadata through."""
    result = CliRunner().invoke(cli, ["sort", "column", str(inp), str(out), "id", "--verbose"])
    assert result.exit_code == 0, result.output
    return result.output


@pytest.fixture
def v2_epoch_orientation_bbox(fields_v2_file, tmp_path):
    """2.0, native geometry, a conventional bbox column, epoch + orientation, no covering."""
    return _write_v2_input(fields_v2_file, tmp_path / "in_epoch.parquet", BASE_GEO)


@pytest.fixture
def v2_with_sidecar(fields_v2_file, tmp_path):
    """The same input plus an unrelated footer key, which also takes the fast path (#756)."""
    return _write_v2_input(
        fields_v2_file,
        tmp_path / "in_sidecar.parquet",
        BASE_GEO,
        extra_kv={"fiboa": FIBOA_VALUE},
    )


class TestFastPathKeepsTheInputsGeoBlock:
    def test_input_takes_the_fast_path(self, v2_epoch_orientation_bbox, tmp_path):
        """Non-vacuity: the assertions below are about the no-rewrite path."""
        output = _sort(v2_epoch_orientation_bbox, tmp_path / "marker.parquet")

        assert FAST_PATH_MARKER in output

    def test_epoch_survives(self, v2_epoch_orientation_bbox, tmp_path):
        out = tmp_path / "epoch.parquet"
        _sort(v2_epoch_orientation_bbox, out)

        assert _geo(out)["columns"]["geometry"]["epoch"] == 2020.5

    def test_orientation_survives(self, v2_epoch_orientation_bbox, tmp_path):
        out = tmp_path / "orientation.parquet"
        _sort(v2_epoch_orientation_bbox, out)

        assert _geo(out)["columns"]["geometry"]["orientation"] == "counterclockwise"

    def test_the_carried_bbox_column_is_declared(self, v2_epoch_orientation_bbox, tmp_path):
        """A bbox column no covering points at costs bytes and tells readers nothing."""
        out = tmp_path / "covering.parquet"
        _sort(v2_epoch_orientation_bbox, out)

        assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names
        assert _geo(out)["columns"]["geometry"]["covering"]["bbox"] == {
            axis: ["bbox", axis] for axis in ("xmin", "ymin", "xmax", "ymax")
        }

    def test_duckdb_generated_fields_are_still_present(self, v2_epoch_orientation_bbox, tmp_path):
        """Carrying the input's block must not lose what DuckDB would have written."""
        out = tmp_path / "generated.parquet"
        _sort(v2_epoch_orientation_bbox, out)

        col = _geo(out)["columns"]["geometry"]
        assert col["encoding"] == "WKB"
        assert col["geometry_types"] == ["Polygon"]
        assert len(col["bbox"]) == 4


class TestSidecarVariant:
    def test_sidecar_input_still_takes_the_fast_path(self, v2_with_sidecar, tmp_path):
        output = _sort(v2_with_sidecar, tmp_path / "sidecar_marker.parquet")

        assert FAST_PATH_MARKER in output

    def test_sidecar_input_keeps_all_three(self, v2_with_sidecar, tmp_path):
        out = tmp_path / "sidecar.parquet"
        _sort(v2_with_sidecar, out)

        col = _geo(out)["columns"]["geometry"]
        assert col["epoch"] == 2020.5
        assert col["orientation"] == "counterclockwise"
        assert "bbox" in col["covering"]

    def test_the_sidecar_key_itself_survives(self, v2_with_sidecar, tmp_path):
        out = tmp_path / "sidecar_key.parquet"
        _sort(v2_with_sidecar, out)

        metadata = pq.ParquetFile(str(out)).schema_arrow.metadata
        assert metadata[b"fiboa"].decode("utf-8") == FIBOA_VALUE


class TestFastPathMatchesTheRewritePath:
    """The strongest guard against this class of bug: the two paths must agree."""

    def _write(self, source: str, destination, force_rewrite: bool) -> dict:
        import geoparquet_io.core.write_strategies as write_strategies
        from geoparquet_io.core.common import write_parquet_with_metadata
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        original = dict(pq.ParquetFile(source).schema_arrow.metadata or {})
        real = write_strategies.needs_metadata_rewrite
        if force_rewrite:
            write_strategies.needs_metadata_rewrite = lambda *a, **k: True
        con = get_duckdb_connection()
        try:
            write_parquet_with_metadata(
                con=con,
                query=f"SELECT * FROM '{source}'",
                output_file=str(destination),
                original_metadata=original,
                compression="ZSTD",
                compression_level=None,
                geoparquet_version="2.0",
            )
        finally:
            con.close()
            write_strategies.needs_metadata_rewrite = real
        return _geo(destination)

    def test_both_paths_produce_the_same_geo_block(self, v2_epoch_orientation_bbox, tmp_path):
        fast = self._write(v2_epoch_orientation_bbox, tmp_path / "fast.parquet", False)
        rewritten = self._write(v2_epoch_orientation_bbox, tmp_path / "rewrite.parquet", True)

        assert fast == rewritten

    def test_the_rewrite_path_really_keeps_all_three(self, v2_epoch_orientation_bbox, tmp_path):
        """Non-vacuity for the parity test: agreeing on a thin block is not a pass."""
        rewritten = self._write(v2_epoch_orientation_bbox, tmp_path / "control.parquet", True)

        col = rewritten["columns"]["geometry"]
        assert col["epoch"] == 2020.5
        assert col["orientation"] == "counterclockwise"
        assert "bbox" in col["covering"]


class TestTheCoveringGuardsAreKept:
    """A covering asserts a relationship gpio cannot verify from a column name (#738)."""

    def test_a_bbox_shaped_column_under_another_name_is_not_declared(
        self, fields_v2_file, tmp_path
    ):
        source = _write_v2_input(
            fields_v2_file, tmp_path / "renamed.parquet", BASE_GEO, bbox_expr="renamed"
        )
        out = tmp_path / "renamed_out.parquet"
        _sort(source, out)

        assert "covering" not in _geo(out)["columns"]["geometry"]

    def test_a_bbox_column_that_is_not_a_struct_is_not_declared(self, fields_v2_file, tmp_path):
        source = _write_v2_input(
            fields_v2_file, tmp_path / "scalar.parquet", BASE_GEO, bbox_expr="not_a_struct"
        )
        out = tmp_path / "scalar_out.parquet"
        _sort(source, out)

        assert "covering" not in _geo(out)["columns"]["geometry"]

    def test_no_bbox_column_means_no_covering(self, fields_v2_file, tmp_path):
        source = _write_v2_input(
            fields_v2_file, tmp_path / "nobbox.parquet", BASE_GEO, bbox_expr=None
        )
        out = tmp_path / "nobbox_out.parquet"
        _sort(source, out)

        col = _geo(out)["columns"]["geometry"]
        assert "covering" not in col
        assert col["epoch"] == 2020.5

    def test_a_declared_covering_still_survives(self, fields_v2_file, tmp_path):
        """#738: an explicitly declared covering is what this carry was built for."""
        geo = json.loads(json.dumps(BASE_GEO))
        geo["columns"]["geometry"]["covering"] = {
            "bbox": {axis: ["bbox", axis] for axis in ("xmin", "ymin", "xmax", "ymax")}
        }
        source = _write_v2_input(fields_v2_file, tmp_path / "declared.parquet", geo)
        out = tmp_path / "declared_out.parquet"
        _sort(source, out)

        assert "bbox" in _geo(out)["columns"]["geometry"]["covering"]


class TestTheCarryDeclines:
    """Cases where DuckDB's own generated block must stand."""

    def _carry(self, metadata, geometry_column="geometry", version="2.0"):
        from geoparquet_io.core.common import _geo_block_to_carry_on_fast_path

        return _geo_block_to_carry_on_fast_path(metadata, geometry_column, version)

    def _metadata(self, column_meta: dict) -> dict:
        return {
            "geo": json.dumps(
                {
                    "version": "2.0.0",
                    "primary_column": "geometry",
                    "columns": {"geometry": column_meta},
                }
            )
        }

    def test_a_block_with_nothing_beyond_duckdbs_own_fields(self):
        assert self._carry(self._metadata({"encoding": "WKB", "geometry_types": ["Point"]})) is None

    def test_a_block_too_thin_to_stand_in(self):
        """Stats a caller invalidated cannot be recomputed here, so decline the carry."""
        assert self._carry(self._metadata({"epoch": 2020.5})) is None

    def test_a_non_2_0_version(self):
        thick = self._metadata({"encoding": "WKB", "geometry_types": ["Point"], "epoch": 2020.5})

        assert self._carry(thick, version="1.1") is None

    def test_no_geometry_column(self):
        thick = self._metadata({"encoding": "WKB", "geometry_types": ["Point"], "epoch": 2020.5})

        assert self._carry(thick, geometry_column=None) is None

    def test_no_original_metadata(self):
        assert self._carry(None) is None

    def test_metadata_without_a_geo_key(self):
        assert self._carry({"fiboa": FIBOA_VALUE}) is None
