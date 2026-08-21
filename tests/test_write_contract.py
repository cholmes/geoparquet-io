"""Write-contract characterization suite (issue #664, suite 4).

Pins the *observable* contract of gpio's write paths: what lands on disk, not
how it got there. Three matrices, decoupled so the cross-product stays small:

* **A1 — path x version.** The four public ways to write a GeoParquet file
  (``write_parquet_with_metadata`` from a query, ``write_geoparquet_table``
  from an Arrow table, ``api.Table.write``, and ``gpio convert geoparquet``)
  are given the same 5-row input and each requested version, and must satisfy
  the *same* expectation. This is the keystone for write-path unification: a
  facade may only merge paths that already agree.
* **A2 — strategy x shape.** The four write strategies against six input
  shapes. ``tests/test_write_strategies.py`` already covers the no-geometry
  shape across all four strategies; that combination is deliberately not
  repeated here.
* **A3 — kv passthrough.** Non-geo file-level key/value metadata (fiboa,
  vecorel, STAC sidecars) surviving each path, plus ``extra_kv_metadata``.

``core/validate.py::validate_geoparquet`` is the oracle — the same pattern
``tests/test_geoparquet_corpus.py`` uses. Assertions are *properties* (row
conservation, primary column, version, bbox, row-group structure) rather than
whole-metadata snapshots, which churn on float formatting and key ordering.
The exception is four tiny normalized geo-dict snapshots, one per version,
which exist only to make a silently added or removed metadata field visible.
Refresh them with ``GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_write_contract.py``.

Policy on disagreement: where paths already disagree, this suite records the
disagreement in ``KNOWN_DIVERGENCES`` / ``KNOWN_STRATEGY_DIVERGENCES`` and
xfails — it does **not** pin either side as correct. Deciding which side wins
is the write-facade's job. A divergence that stops reproducing fails loudly so
the stale entry gets removed; anything *new* fails hard rather than being
absorbed. Every reason string below was found by building this suite; together
they are the write-facade decision list.
"""

from __future__ import annotations

import json
import os
import struct
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.api import read as api_read
from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import (
    get_duckdb_connection,
    get_parquet_metadata,
    write_geoparquet_table,
    write_parquet_with_metadata,
)
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

SNAPSHOT_DIR = Path(__file__).parent / "data" / "snapshots"
UPDATE_SNAPSHOTS = os.environ.get("GPIO_UPDATE_SNAPSHOT") == "1"

# ---------------------------------------------------------------------------
# Known cross-path divergences — recorded, not pinned (see module docstring).
# ---------------------------------------------------------------------------

CLI_1_0_COVERING = (
    "gpio convert geoparquet adds a bbox covering column at every version, so "
    "--geoparquet-version 1.0 emits a 1.0.0 file carrying the 1.1-only "
    "'covering' key and fails gpio's own spec check. The other three write "
    "paths add no covering. Write-facade decision needed (#664)."
)

WGT_IGNORES_PARQUET_GEO_ONLY = (
    "write_geoparquet_table ignores geoparquet_version='parquet-geo-only': it "
    "writes a native GEOMETRY logical type *and* stamps 1.1.0 'geo' metadata, "
    "so the file claims 1.1 while using a 2.0-only feature. The other three "
    "paths omit the geo key entirely. Write-facade decision needed (#664)."
)

STREAMING_GEOMETRY_TYPE = (
    "The arrow-streaming strategy's on-disk geometry type depends on global "
    "process state: without geoarrow.pyarrow imported it writes LARGE_BINARY "
    "(fails geometry_byte_array), and once anything in the process has "
    "imported geoarrow.pyarrow it writes a native geoarrow.wkb extension type "
    "under a 1.1.0 declaration (fails version_features_match). Either way the "
    "output fails gpio's own validator, and which failure you get is decided "
    "by import order. See test_streaming_output_is_independent_of_geoarrow_import (#664)."
)

GEOARROW_ENCODING_UNVALIDATABLE = (
    "1.1-geoarrow output is rejected by gpio's own validator: validate.py "
    "treats 'point'/'linestring'/... GeoArrow encodings as invalid and then "
    "tries ST_GeomFromWKB on the struct column. GeoParquet 1.1 permits these "
    "encodings, so this is a validator gap, not a writer bug (#664)."
)

# (path, version) -> reason. A1 only.
KNOWN_DIVERGENCES: dict[tuple[str, str], str] = {
    ("cli", "1.0"): CLI_1_0_COVERING,
    ("write_geoparquet_table", "parquet-geo-only"): WGT_IGNORES_PARQUET_GEO_ONLY,
}

DISK_REWRITE_IGNORES_ROW_GROUP_ROWS = (
    "The disk-rewrite strategy accepts row_group_rows and row_group_size_mb "
    "and uses neither: write_from_query issues a bare COPY TO and rewrites "
    "with PyArrow defaults, so a caller's row-group sizing is silently "
    "dropped. The other three strategies honour it. Write-facade decision "
    "needed (#664)."
)

# (strategy, shape) -> reason. A2 only.
KNOWN_STRATEGY_DIVERGENCES: dict[tuple[str, str], str] = {
    ("streaming", shape): STREAMING_GEOMETRY_TYPE
    for shape in ("normal", "zero_row", "null_geoms", "zm", "geom_named", "multi_row_group")
}
KNOWN_STRATEGY_DIVERGENCES[("disk-rewrite", "multi_row_group")] = (
    DISK_REWRITE_IGNORES_ROW_GROUP_ROWS
)

# Oracle failures the streaming divergence can produce, in either global state.
# Prefixes: per-column checks are named "<check>_<geometry column>".
STREAMING_ALLOWED_FAILURES = ("geometry_byte_array", "version_features_match")

# (path, version) -> reason, for paths that drop input kv metadata. A3 only.
KV_DROPPED = (
    "Input non-geo kv metadata (fiboa/vecorel/STAC sidecars) is preserved by "
    "write_parquet_with_metadata and write_geoparquet_table but dropped by "
    "api.Table.write and gpio convert geoparquet. Write-facade decision "
    "needed — silent metadata loss on the two highest-level entry points (#664)."
)

# ---------------------------------------------------------------------------
# Fixture data: built in-test, no new files under tests/data.
# ---------------------------------------------------------------------------

POINTS = [(-122.4, 37.8), (-74.0, 40.7), (2.35, 48.86), (139.7, 35.7), (151.2, -33.9)]
BBOX = (-122.4, -33.9, 151.2, 48.86)
EXTRA_KV = {"fiboa": '{"schemas": ["example"]}', "custom_note": '{"hello": "world"}'}

VERSIONS = ["1.0", "1.1", "2.0", "parquet-geo-only"]
GEO_VERSION_FOR = {"1.0": "1.0.0", "1.1": "1.1.0", "2.0": "2.0.0", "parquet-geo-only": None}
STRATEGIES = ["duckdb-kv", "streaming", "in-memory", "disk-rewrite"]


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _wkb_point_zm(x: float, y: float, z: float, m: float) -> bytes:
    return struct.pack("<BI4d", 1, 3001, x, y, z, m)


def _geo_dict(column: str = "geometry", geometry_types: tuple[str, ...] = ("Point",)) -> dict:
    return {
        "version": "1.1.0",
        "primary_column": column,
        "columns": {column: {"encoding": "WKB", "geometry_types": list(geometry_types)}},
    }


def _write_source(
    path: Path,
    table: pa.Table,
    geo: dict,
    extra_kv: dict[str, str] | None = None,
    row_group_size: int | None = None,
) -> str:
    metadata = {b"geo": json.dumps(geo).encode()}
    for key, value in (extra_kv or {}).items():
        metadata[key.encode()] = value.encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, path, row_group_size=row_group_size)
    return str(path)


@pytest.fixture(scope="module")
def normal_source(tmp_path_factory) -> str:
    """The one shared A1/A3 input: 5 WKB points, two attribute columns."""
    tmp = tmp_path_factory.mktemp("write_contract")
    table = pa.table(
        {
            "id": list(range(1, 6)),
            "name": ["a", "b", "c", "d", "e"],
            "geometry": [_wkb_point(x, y) for x, y in POINTS],
        }
    )
    return _write_source(tmp / "normal.parquet", table, _geo_dict(), extra_kv=EXTRA_KV)


@pytest.fixture(scope="module")
def shape_sources(tmp_path_factory) -> dict[str, str]:
    """One input file per A2 shape."""
    tmp = tmp_path_factory.mktemp("write_contract_shapes")
    geoms = [_wkb_point(x, y) for x, y in POINTS]
    sources = {}

    sources["normal"] = _write_source(
        tmp / "normal.parquet",
        pa.table({"id": list(range(5)), "geometry": geoms}),
        _geo_dict(),
    )
    sources["zero_row"] = _write_source(
        tmp / "zero_row.parquet",
        pa.table({"id": pa.array([], type=pa.int64()), "geometry": pa.array([], type=pa.binary())}),
        _geo_dict(geometry_types=()),
    )
    nullable = list(geoms)
    nullable[1] = None
    nullable[3] = None
    sources["null_geoms"] = _write_source(
        tmp / "null_geoms.parquet",
        pa.table({"id": list(range(5)), "geometry": nullable}),
        _geo_dict(),
    )
    sources["zm"] = _write_source(
        tmp / "zm.parquet",
        pa.table(
            {
                "id": list(range(5)),
                "geometry": [
                    _wkb_point_zm(x, y, 10.0 + i, float(i)) for i, (x, y) in enumerate(POINTS)
                ],
            }
        ),
        _geo_dict(geometry_types=("Point ZM",)),
    )
    sources["geom_named"] = _write_source(
        tmp / "geom_named.parquet",
        pa.table({"id": list(range(5)), "geom": geoms}),
        _geo_dict(column="geom"),
    )
    sources["multi_row_group"] = _write_source(
        tmp / "multi_row_group.parquet",
        pa.table({"id": list(range(40)), "geometry": geoms * 8}),
        _geo_dict(),
        row_group_size=10,
    )
    return sources


# Rows, geometry column and declared types per A2 shape.
SHAPE_EXPECTATIONS = {
    "normal": (5, "geometry", ("Point",), BBOX),
    "zero_row": (0, "geometry", (), None),
    "null_geoms": (5, "geometry", ("Point",), BBOX),
    "zm": (5, "geometry", ("Point ZM",), BBOX),
    "geom_named": (5, "geom", ("Point",), BBOX),
    "multi_row_group": (40, "geometry", ("Point",), BBOX),
}


# ---------------------------------------------------------------------------
# The contract assertion helper
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Expect:
    """The observable contract one write must satisfy."""

    rows: int
    geometry_column: str = "geometry"
    #: geo metadata "version" string, or None when no 'geo' key may be written
    geo_version: str | None = "1.1.0"
    geometry_types: tuple[str, ...] | None = ("Point",)
    bbox: tuple[float, float, float, float] | None = BBOX
    encoding: str | None = "WKB"
    #: columns that must survive the write (extras such as a covering bbox are allowed)
    columns: tuple[str, ...] = ()
    #: file-level kv keys that must be present besides 'geo'
    kv_keys: tuple[str, ...] = ()
    #: exact row-group count, when the caller pinned row_group_rows
    row_groups: int | None = None
    known_bug_checks: frozenset[str] = field(default_factory=frozenset)


def _geo_metadata(path: str) -> dict | None:
    pf = pq.ParquetFile(path)
    try:
        raw = (pf.schema_arrow.metadata or {}).get(b"geo")
    finally:
        pf.close()
    return json.loads(raw) if raw else None


def assert_valid_geoparquet_output(path: str, expect: Expect) -> list[str]:
    """Assert the write contract; return the oracle's FAILED check names.

    Structural violations raise immediately. Oracle failures are *returned* so
    the caller can adjudicate them against the known-divergence tables instead
    of losing the structural coverage to an early xfail.
    """
    pf = pq.ParquetFile(path)
    try:
        assert pf.metadata.num_rows == expect.rows, (
            f"row conservation broken: wrote {pf.metadata.num_rows} rows, expected {expect.rows}"
        )
        column_names = list(pf.schema_arrow.names)
        kv = pf.schema_arrow.metadata or {}
        row_group_rows = [
            pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)
        ]
        num_row_groups = pf.metadata.num_row_groups
    finally:
        pf.close()

    missing = [c for c in expect.columns if c not in column_names]
    assert not missing, f"write dropped input columns {missing} (kept {column_names})"

    # Row-group structure: every row must live in exactly one row group, and a
    # pinned row-group count must be honoured.
    assert sum(row_group_rows) == expect.rows, (
        f"row groups hold {sum(row_group_rows)} rows but the file claims {expect.rows}"
    )
    if expect.row_groups is not None:
        assert num_row_groups == expect.row_groups, (
            f"expected {expect.row_groups} row groups, got {num_row_groups} ({row_group_rows})"
        )

    missing_kv = [k for k in expect.kv_keys if k.encode() not in kv]
    assert not missing_kv, (
        f"kv metadata {missing_kv} dropped by the write (kept {sorted(k.decode() for k in kv)})"
    )

    geo = json.loads(kv[b"geo"]) if b"geo" in kv else None
    if expect.geo_version is None:
        assert geo is None, (
            f"expected no 'geo' metadata key, got version {geo.get('version')!r}"  # type: ignore[union-attr]
        )
    else:
        assert geo is not None, "write produced no 'geo' metadata key"
        assert geo.get("version") == expect.geo_version, (
            f"geo version {geo.get('version')!r}, expected {expect.geo_version!r}"
        )
        assert geo.get("primary_column") == expect.geometry_column, (
            f"primary_column {geo.get('primary_column')!r}, expected {expect.geometry_column!r}"
        )
        column_meta = geo.get("columns", {}).get(expect.geometry_column)
        assert column_meta is not None, (
            f"geo.columns has no entry for {expect.geometry_column!r} "
            f"(has {sorted(geo.get('columns', {}))})"
        )
        if expect.encoding is not None:
            assert column_meta.get("encoding") == expect.encoding, (
                f"encoding {column_meta.get('encoding')!r}, expected {expect.encoding!r}"
            )
        if expect.geometry_types is not None:
            assert tuple(column_meta.get("geometry_types", ())) == expect.geometry_types, (
                f"geometry_types {column_meta.get('geometry_types')!r}, "
                f"expected {list(expect.geometry_types)!r}"
            )
        if expect.bbox is not None:
            written = column_meta.get("bbox")
            assert written is not None, "geo metadata carries no bbox"
            assert written == pytest.approx(list(expect.bbox), abs=1e-9), (
                f"bbox {written!r}, expected {list(expect.bbox)!r}"
            )

    result = validate_geoparquet(path, validate_data=True, sample_size=0)
    return [c.name for c in result.checks if c.status == CheckStatus.FAILED]


def _adjudicate(
    failed: list[str], reason: str | None, allowed: tuple[str, ...] | None = None
) -> None:
    """Xfail on a recorded divergence; fail hard on anything new or stale."""
    if failed:
        assert reason is not None, f"unexpected validation failures: {failed}"
        if allowed is not None:
            unexpected = [n for n in failed if not any(n.startswith(p) for p in allowed)]
            assert not unexpected, (
                f"validation failures beyond the recorded divergence: {unexpected}"
            )
        pytest.xfail(reason)
    if reason is not None:
        pytest.fail(
            "recorded divergence no longer reproduces — the write paths now agree. "
            f"Remove the entry so it cannot mask a regression.\nEntry: {reason}"
        )


# ---------------------------------------------------------------------------
# The four write paths, behind one signature
# ---------------------------------------------------------------------------


def _write_via_query(source: str, out: str, version: str, **kwargs) -> None:
    con = get_duckdb_connection()
    try:
        metadata, _ = get_parquet_metadata(source)
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet('{Path(source).as_posix()}')",
            out,
            original_metadata=metadata,
            geoparquet_version=version,
            input_file=source,
            **kwargs,
        )
    finally:
        con.close()


def _write_via_table(source: str, out: str, version: str, **kwargs) -> None:
    write_geoparquet_table(pq.read_table(source), out, geoparquet_version=version, **kwargs)


def _write_via_api(source: str, out: str, version: str, **kwargs) -> None:
    api_read(source).write(out, geoparquet_version=version, **kwargs)


def _write_via_cli(source: str, out: str, version: str, **kwargs) -> None:
    result = CliRunner().invoke(
        cli,
        [
            "convert",
            "geoparquet",
            source,
            out,
            "--skip-hilbert",
            "--geoparquet-version",
            version,
        ],
    )
    assert result.exit_code == 0, f"CLI failed ({result.exit_code}):\n{result.output}"


WRITE_PATHS = {
    "write_parquet_with_metadata": _write_via_query,
    "write_geoparquet_table": _write_via_table,
    "api_table_write": _write_via_api,
    "cli": _write_via_cli,
}


# ---------------------------------------------------------------------------
# A1 — path x version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("path_name", sorted(WRITE_PATHS))
def test_a1_path_version_contract(path_name, version, normal_source, tmp_path):
    """Every write path must satisfy the same contract for the same input."""
    out = tmp_path / f"{path_name}_{version}.parquet"
    expect = Expect(
        rows=5,
        geo_version=GEO_VERSION_FOR[version],
        columns=("id", "name", "geometry"),
    )
    reason = KNOWN_DIVERGENCES.get((path_name, version))

    try:
        WRITE_PATHS[path_name](normal_source, str(out), version)
        failed = assert_valid_geoparquet_output(str(out), expect)
    except AssertionError:
        if reason is not None:
            pytest.xfail(reason)
        raise
    _adjudicate(failed, reason)


# ---------------------------------------------------------------------------
# A2 — strategy x shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", sorted(SHAPE_EXPECTATIONS))
@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a2_strategy_shape_contract(strategy, shape, shape_sources, tmp_path):
    """Every strategy must produce the same file for every input shape.

    The no-geometry shape is covered by tests/test_write_strategies.py and is
    deliberately not repeated here.
    """
    rows, geometry_column, geometry_types, bbox = SHAPE_EXPECTATIONS[shape]
    source = shape_sources[shape]
    out = tmp_path / f"{strategy}_{shape}.parquet"
    # Pin the row-group count only where there is more than one group's worth.
    row_group_rows = 10 if shape == "multi_row_group" else None

    _write_via_query(
        source, str(out), "1.1", write_strategy=strategy, row_group_rows=row_group_rows
    )
    expect = Expect(
        rows=rows,
        geometry_column=geometry_column,
        geo_version="1.1.0",
        geometry_types=geometry_types,
        bbox=bbox,
        columns=("id", geometry_column),
        row_groups=4 if shape == "multi_row_group" else None,
    )
    reason = KNOWN_STRATEGY_DIVERGENCES.get((strategy, shape))
    allowed = STREAMING_ALLOWED_FAILURES if strategy == "streaming" else None
    try:
        failed = assert_valid_geoparquet_output(str(out), expect)
    except AssertionError:
        if reason is not None:
            pytest.xfail(reason)
        raise
    _adjudicate(failed, reason, allowed)


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a2_geoarrow_version_agrees_across_strategies(strategy, shape_sources, tmp_path):
    """1.1-geoarrow must not depend on which strategy the caller asked for.

    The issue records "1.1-geoarrow is only writable by the streaming
    strategy" as a known asymmetry. That is no longer true:
    ``write_parquet_with_metadata`` auto-routes a WKB input to arrow-streaming
    whatever the caller asked for, so all four requests now produce the same
    natively encoded file. What remains broken is downstream — gpio's own
    validator rejects the result.
    """
    out = tmp_path / f"geoarrow_{strategy}.parquet"
    _write_via_query(shape_sources["normal"], str(out), "1.1-geoarrow", write_strategy=strategy)
    expect = Expect(
        rows=5,
        geo_version="1.1.0",
        encoding="point",
        columns=("id",),  # the WKB column is replaced by a nested x/y struct
    )
    failed = assert_valid_geoparquet_output(str(out), expect)
    _adjudicate(failed, GEOARROW_ENCODING_UNVALIDATABLE)


# ---------------------------------------------------------------------------
# A3 — kv / settings passthrough
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path_name", sorted(WRITE_PATHS))
def test_a3_input_kv_metadata_survives(path_name, normal_source, tmp_path):
    """Non-geo file-level kv metadata on the input must reach the output."""
    out = tmp_path / f"kv_{path_name}.parquet"
    WRITE_PATHS[path_name](normal_source, str(out), "1.1")

    pf = pq.ParquetFile(out)
    try:
        written = {k.decode() for k in (pf.schema_arrow.metadata or {})}
    finally:
        pf.close()

    dropped = sorted(set(EXTRA_KV) - written)
    if dropped and path_name in ("api_table_write", "cli"):
        pytest.xfail(f"{KV_DROPPED} (dropped: {dropped})")
    assert not dropped, f"{path_name} dropped input kv metadata {dropped}"
    if path_name in ("api_table_write", "cli"):
        pytest.fail(
            "recorded divergence no longer reproduces — "
            f"{path_name} now preserves input kv metadata; remove the KV_DROPPED entry"
        )


def test_a3_extra_kv_metadata_written(normal_source, tmp_path):
    """extra_kv_metadata lands alongside 'geo' and does not evict input keys."""
    out = tmp_path / "extra_kv.parquet"
    _write_via_query(
        normal_source,
        str(out),
        "1.1",
        extra_kv_metadata={"stac": json.dumps({"type": "Feature"})},
    )
    pf = pq.ParquetFile(out)
    try:
        kv = {k.decode(): v.decode() for k, v in (pf.schema_arrow.metadata or {}).items()}
    finally:
        pf.close()

    assert "stac" in kv, f"extra_kv_metadata not written (keys: {sorted(kv)})"
    assert json.loads(kv["stac"]) == {"type": "Feature"}
    assert set(EXTRA_KV).issubset(kv), "extra_kv_metadata evicted the input's own kv keys"
    assert "geo" in kv


# ---------------------------------------------------------------------------
# Normalized geo-dict snapshots — canaries for silent field drift
# ---------------------------------------------------------------------------


def _normalize_geo(geo: dict | None) -> dict:
    """Stable, diff-friendly view: sorted keys, bbox rounded, CRS collapsed."""
    if geo is None:
        return {"geo": None}
    normalized = {k: v for k, v in geo.items() if k != "columns"}
    columns = {}
    for name, meta in sorted(geo.get("columns", {}).items()):
        entry = {}
        for key, value in sorted(meta.items()):
            if key == "bbox" and value is not None:
                entry[key] = [round(float(v), 6) for v in value]
            elif key == "crs":
                # PROJJSON is huge and pyproj-version-sensitive; its identity is
                # covered by tests/test_crs_*.py. Record only presence + name.
                entry[key] = value.get("id") if isinstance(value, dict) else value
            else:
                entry[key] = value
        columns[name] = entry
    normalized["columns"] = columns
    return normalized


@pytest.mark.parametrize("version", VERSIONS)
def test_geo_metadata_snapshot(version, normal_source, tmp_path):
    """A field silently added to or removed from geo metadata must show up here."""
    out = tmp_path / f"snapshot_{version}.parquet"
    _write_via_query(normal_source, str(out), version)
    actual = _normalize_geo(_geo_metadata(str(out)))
    rendered = json.dumps(actual, indent=2, sort_keys=True) + "\n"

    snapshot = SNAPSHOT_DIR / f"write_contract_geo_{version}.json"
    if UPDATE_SNAPSHOTS or not snapshot.exists():
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_text(rendered)
        if not UPDATE_SNAPSHOTS:
            pytest.fail(f"snapshot {snapshot.name} was missing; wrote it — re-run to verify")
        return

    assert rendered == snapshot.read_text(), (
        f"geo metadata for version {version} drifted from {snapshot.name}. "
        "If the change is intended, refresh with "
        "GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_write_contract.py"
    )


# ---------------------------------------------------------------------------
# Global-state independence (the highest-value finding in this suite)
# ---------------------------------------------------------------------------


_STREAMING_PROBE = """
import json, struct, sys, tempfile
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq
if "--import-geoarrow" in sys.argv:
    import geoarrow.pyarrow  # noqa: F401
from geoparquet_io.core.common import (
    get_duckdb_connection, get_parquet_metadata, write_parquet_with_metadata,
)
geo = {"version": "1.1.0", "primary_column": "geometry",
       "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}}}
tmp = Path(tempfile.mkdtemp())
src = tmp / "src.parquet"
table = pa.table({"id": [1, 2], "geometry": [struct.pack("<BI2d", 1, 1, 1.0, 2.0),
                                             struct.pack("<BI2d", 1, 1, 3.0, 4.0)]})
pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(geo).encode()}), src)
out = tmp / "out.parquet"
con = get_duckdb_connection()
meta, _ = get_parquet_metadata(str(src))
write_parquet_with_metadata(
    con, f"SELECT * FROM read_parquet('{src.as_posix()}')", str(out),
    original_metadata=meta, geoparquet_version="1.1",
    write_strategy="streaming", input_file=str(src),
)
con.close()
pf = pq.ParquetFile(out)
print("GEOMETRY_TYPE:" + str(pf.schema_arrow.field("geometry").type))
pf.close()
"""


def _run_streaming_probe(*args: str) -> str:
    proc = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_STREAMING_PROBE), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"probe failed:\n{proc.stdout}\n{proc.stderr}"
    for line in proc.stdout.splitlines():
        if line.startswith("GEOMETRY_TYPE:"):
            return line.split(":", 1)[1]
    raise AssertionError(f"probe printed no geometry type:\n{proc.stdout}")


@pytest.mark.xfail(strict=False, reason=STREAMING_GEOMETRY_TYPE)
def test_streaming_output_is_independent_of_geoarrow_import():
    """The same write must produce the same file whatever else the process imported.

    Two subprocesses, identical except that one imports ``geoarrow.pyarrow``
    first. Today they disagree: ``large_binary`` versus a native
    ``geoarrow.wkb`` extension type, which is why the streaming rows of A2 land
    on two different validator failures depending on test ordering. Marked
    non-strict xfail: this asserts the *desired* behavior, so it flips to XPASS
    when the leak is fixed rather than pinning the bug.
    """
    without = _run_streaming_probe()
    with_geoarrow = _run_streaming_probe("--import-geoarrow")
    assert without == with_geoarrow, (
        f"streaming wrote {without!r} on a clean interpreter but {with_geoarrow!r} "
        "after geoarrow.pyarrow was imported"
    )
