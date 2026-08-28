"""GeoParquet 2.0 outputs must keep their spatial access metadata.

GeoParquet 2.0 gets row-group bounds for free from the native Parquet
GEOMETRY/GEOGRAPHY geospatial statistics, and it keeps 1.1's optional ``bbox``
covering (opengeospatial/geoparquet#302) for page-level pruning. Writes that
took the 2.0 "no metadata rewrite needed" fast path let DuckDB regenerate the
``geo`` key from scratch, which silently dropped the covering — a bbox column
that costs bytes and tells readers nothing (issue #738).

These tests pin both halves of the contract:

- a bbox column in a 2.0 output is always described by a ``covering`` entry
- a 2.0 output always carries native geospatial statistics
"""

from __future__ import annotations

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import get_geo_metadata

BBOX_COVERING = {
    "xmin": ["bbox", "xmin"],
    "ymin": ["bbox", "ymin"],
    "xmax": ["bbox", "xmax"],
    "ymax": ["bbox", "ymax"],
}


def _points_table(count: int = 400) -> pa.Table:
    side = int(count**0.5)
    pts = [shapely.Point(x / 10.0, y / 10.0) for x in range(side) for y in range(side)]
    return pa.table(
        {
            "id": pa.array(range(len(pts))),
            "geometry": pa.array([shapely.to_wkb(p) for p in pts], pa.binary()),
        }
    )


def _bbox_struct(table: pa.Table) -> pa.Array:
    geoms = [shapely.from_wkb(v.as_py()) for v in table.column("geometry")]
    bounds = [g.bounds for g in geoms]
    return pa.StructArray.from_arrays(
        [
            pa.array([b[0] for b in bounds], pa.float64()),
            pa.array([b[1] for b in bounds], pa.float64()),
            pa.array([b[2] for b in bounds], pa.float64()),
            pa.array([b[3] for b in bounds], pa.float64()),
        ],
        names=["xmin", "ymin", "xmax", "ymax"],
    )


def _write_v2_wkb(path, with_bbox_column: bool = False, with_covering: bool = False) -> str:
    """Write a WKB-encoded GeoParquet 2.0.0 file, optionally with a bbox column."""
    table = _points_table()
    col_meta: dict = {
        "encoding": "WKB",
        "geometry_types": ["Point"],
        "crs": {"id": {"authority": "OGC", "code": "CRS84"}},
    }
    if with_bbox_column:
        table = table.append_column("bbox", _bbox_struct(table))
    if with_covering:
        col_meta["covering"] = {"bbox": BBOX_COVERING}
    geo = {"version": "2.0.0", "primary_column": "geometry", "columns": {"geometry": col_meta}}
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, str(path))
    return str(path)


def _geometry_column_meta(path) -> dict:
    geo = get_geo_metadata(str(path))
    assert geo is not None, f"{path} has no geo metadata"
    return geo["columns"][geo["primary_column"]]


def _has_native_geo_statistics(path) -> bool:
    """True when the geometry column chunk carries Parquet geospatial statistics."""
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [str(path)],
        ).fetchall()
    finally:
        con.close()
    return bool(rows) and all(row[0] is not None for row in rows)


def _run(*args) -> None:
    result = CliRunner().invoke(cli, [str(a) for a in args])
    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# covering is written for a bbox column the command itself adds
# ---------------------------------------------------------------------------


def test_sort_hilbert_add_bbox_writes_covering_at_v2(tmp_path):
    """#738: --add-bbox on a 2.0 input must describe the bbox column it adds."""
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--add-bbox")

    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names
    assert get_geo_metadata(str(out))["version"] == "2.0.0"
    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


@pytest.mark.parametrize("strategy", ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"])
def test_every_write_strategy_declares_the_bbox_column_at_v2(tmp_path, strategy):
    """The covering must survive whichever strategy actually writes the file."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run("extract", "geoparquet", src, out, "--write-strategy", strategy)

    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


# ---------------------------------------------------------------------------
# covering carried on the input survives a 2.0 -> 2.0 rewrite
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        ["sort", "hilbert"],
        ["sort", "quadkey"],
        ["extract", "geoparquet"],
    ],
)
def test_existing_covering_survives_v2_roundtrip(tmp_path, command):
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run(*command, src, out)

    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names
    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


def test_bbox_column_without_covering_gains_one_at_v2(tmp_path):
    """A 2.0 input whose bbox column is undeclared comes out declared."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=False)
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out)

    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


# ---------------------------------------------------------------------------
# spatial statistics on every 2.0 output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("add_bbox", [True, False])
def test_v2_output_has_native_geo_statistics(tmp_path, add_bbox):
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    args = ["sort", "hilbert", src, out]
    if add_bbox:
        args.append("--add-bbox")
    _run(*args)

    assert _has_native_geo_statistics(out)


def test_explicit_v2_output_has_native_geo_statistics(tmp_path):
    """A 1.1 input asked for 2.0 output gets the native types (and their stats)."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--geoparquet-version", "2.0")

    assert _has_native_geo_statistics(out)
    assert _geometry_column_meta(out)["covering"] == {"bbox": BBOX_COVERING}


# ---------------------------------------------------------------------------
# parquet-geo-only must stay metadata-free
# ---------------------------------------------------------------------------


def test_parquet_geo_only_output_gains_no_geo_metadata(tmp_path):
    """A bbox column must not talk gpio into writing a 'geo' key for pgo output."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--geoparquet-version", "parquet-geo-only")

    metadata = pq.ParquetFile(str(out)).metadata.metadata or {}
    assert b"geo" not in metadata


# ---------------------------------------------------------------------------
# auto mode must not advise a version the output already has
# ---------------------------------------------------------------------------


def test_auto_mode_on_v2_input_does_not_advise_upgrading_to_v2(tmp_path):
    """#738: the v1.1 pushdown warning fired on 2.0 output, hiding the real gap."""
    from unittest.mock import patch

    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
        from geoparquet_io.core.hilbert_order import hilbert_order

        hilbert_order(src, str(out), geoparquet_version=None)

    assert get_geo_metadata(str(out))["version"] == "2.0.0"
    for call in mock_warn.call_args_list:
        assert "no spatial filter pushdown" not in str(call)


# ---------------------------------------------------------------------------
# `gpio check` must not fight the covering it now writes
# ---------------------------------------------------------------------------


def _check_bbox(path) -> dict:
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    return check_metadata_and_bbox(str(path), verbose=False, return_results=True)


def test_check_accepts_a_declared_bbox_covering_at_v2(tmp_path):
    """2.0 keeps the optional bbox covering, so a declared one is not a defect."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=True)

    result = _check_bbox(src)

    assert result["file_type"] == "geoparquet_v2"
    assert result["has_bbox_column"] is True
    assert result["passed"] is True
    assert result["needs_bbox_removal"] is False
    assert result["fix_available"] is False
    assert result["issues"] == []


def test_check_flags_an_undeclared_bbox_column_at_v2(tmp_path):
    """The #738 shape: bytes on disk that no covering points at."""
    src = _write_v2_wkb(tmp_path / "in.parquet", with_bbox_column=True, with_covering=False)

    result = _check_bbox(src)

    assert result["passed"] is False
    assert result["needs_bbox_removal"] is True
    assert result["fix_available"] is True
    assert "covering" in result["issues"][0]


def test_sort_hilbert_add_bbox_output_passes_check_at_v2(tmp_path):
    """End to end: what gpio writes, gpio accepts."""
    src = _write_v2_wkb(tmp_path / "in.parquet")
    out = tmp_path / "out.parquet"

    _run("sort", "hilbert", src, out, "--add-bbox")

    assert _check_bbox(out)["passed"] is True
