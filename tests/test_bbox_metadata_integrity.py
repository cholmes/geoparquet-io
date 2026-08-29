"""`gpio add bbox-metadata` must stay metadata-only and refuse plain Parquet.

Two defects found during the #686 review, fixed together because both live in
`core/add/bbox_metadata.py` and both end in "reports success, writes a file its
own validator rejects":

- #712: on a 1.x WKB input the rewrite silently re-materialized the geometry
  column as a native Parquet GEOMETRY logical type while leaving the declared
  version at 1.1.0.
- #713: on plain Parquet with a bbox column it invented a 1.1.0 `geo` block
  containing only `covering` -- no `encoding`, no `geometry_types`.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.validate import validate_geoparquet

POINTS = "(1, ST_GeomFromText('POINT (30 10)')), (2, ST_GeomFromText('POINT (40 40)'))"


def _write(path, geo=None, native=False):
    """Write a two-point file with a bbox struct column.

    ``native`` writes a native Parquet GEOMETRY column (2.0); otherwise the
    geometry is a plain BLOB carrying WKB, as a 1.x file must have.
    """
    kv = f", KV_METADATA {{geo: '{geo}'}}" if geo else ""
    geometry_expr = "geom" if native else "ST_AsWKB(geom)::BLOB"
    version = "'V2'" if native else "'NONE'"
    con = get_duckdb_connection(load_spatial=True)
    try:
        con.execute(f"""
            COPY (
              SELECT
                id,
                {geometry_expr} AS geometry,
                {{
                  'xmin': ST_XMin(geom), 'ymin': ST_YMin(geom),
                  'xmax': ST_XMax(geom), 'ymax': ST_YMax(geom)
                }} AS bbox
              FROM (VALUES {POINTS}) t(id, geom)
            ) TO '{path.as_posix()}'
            (FORMAT PARQUET, GEOPARQUET_VERSION {version}{kv})
        """)
    finally:
        con.close()
    return path


def _geo_key(path):
    metadata = pq.read_metadata(str(path)).metadata
    return json.loads(metadata[b"geo"].decode("utf-8")) if metadata and b"geo" in metadata else None


def _geometry_logical_type(path):
    """The Parquet *logical* type of the geometry column: None for plain WKB."""
    con = get_duckdb_connection(load_spatial=False)
    try:
        row = con.execute(
            f"SELECT logical_type FROM parquet_schema('{path}') WHERE name = 'geometry'"
        ).fetchone()
    finally:
        con.close()
    return str(row[0]) if row and row[0] else None


@pytest.fixture
def v11_wkb_with_bbox(tmp_path):
    """A valid 1.1 file: WKB geometry, bbox column, no covering."""
    geo = json.dumps(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "bbox": [30.0, 10.0, 40.0, 40.0],
                }
            },
        }
    )
    path = _write(tmp_path / "v11.parquet", geo=geo)
    assert validate_geoparquet(str(path)).is_valid
    assert _geometry_logical_type(path) is None, "fixture must start as plain WKB"
    return path


@pytest.fixture
def v20_native_with_bbox(tmp_path):
    """A valid 2.0 file: native GEOMETRY column, bbox column, no covering."""
    geo = json.dumps(
        {
            "version": "2.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
        }
    )
    path = _write(tmp_path / "v20.parquet", geo=geo, native=True)
    assert _geometry_logical_type(path) is not None, "fixture must start native"
    return path


@pytest.fixture
def plain_parquet_with_bbox(tmp_path):
    """Ordinary Parquet: no geo metadata at all, but it does have a bbox column."""
    path = _write(tmp_path / "plain.parquet")
    assert pq.read_metadata(str(path)).metadata is None
    return path


class TestKeepsTheGeometryColumnPhysicalType:
    """#712: a metadata-only command must not re-type the geometry column."""

    def test_v11_wkb_stays_wkb(self, v11_wkb_with_bbox):
        add_bbox_metadata(str(v11_wkb_with_bbox))

        assert _geometry_logical_type(v11_wkb_with_bbox) is None, (
            "1.x WKB geometry was rewritten as a native Parquet GEOMETRY type"
        )

    def test_v11_output_is_valid_geoparquet(self, v11_wkb_with_bbox):
        """The whole point: the file its own validator accepted still passes."""
        add_bbox_metadata(str(v11_wkb_with_bbox))

        result = validate_geoparquet(str(v11_wkb_with_bbox))
        failed = [c.message for c in result.checks if c.status.value == "failed"]
        assert result.is_valid, f"add bbox-metadata produced an invalid file: {failed}"

    def test_v11_version_and_covering_are_both_right(self, v11_wkb_with_bbox):
        add_bbox_metadata(str(v11_wkb_with_bbox))

        geo = _geo_key(v11_wkb_with_bbox)
        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_v11_data_survives_the_rewrite(self, v11_wkb_with_bbox):
        """The BLOB cast must not drop or mangle rows."""
        add_bbox_metadata(str(v11_wkb_with_bbox))

        table = pq.read_table(str(v11_wkb_with_bbox))
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]

    def test_native_input_stays_native(self, v20_native_with_bbox):
        """A 2.0 input keeps its native type — that case was never broken."""
        add_bbox_metadata(str(v20_native_with_bbox))

        assert _geometry_logical_type(v20_native_with_bbox) is not None
        assert validate_geoparquet(str(v20_native_with_bbox)).is_valid
        assert _geo_key(v20_native_with_bbox)["version"] == "2.0.0"


class TestRefusesPlainParquet:
    """#713: don't invent a `geo` block that fails five of its own spec checks."""

    def test_raises_instead_of_reporting_success(self, plain_parquet_with_bbox):
        with pytest.raises(GeoParquetError, match="no GeoParquet metadata"):
            add_bbox_metadata(str(plain_parquet_with_bbox))

    def test_error_names_the_command_that_does_work(self, plain_parquet_with_bbox):
        with pytest.raises(GeoParquetError, match="gpio convert geoparquet"):
            add_bbox_metadata(str(plain_parquet_with_bbox))

    def test_file_is_left_untouched(self, plain_parquet_with_bbox):
        before = pq.read_table(str(plain_parquet_with_bbox))
        with pytest.raises(GeoParquetError):
            add_bbox_metadata(str(plain_parquet_with_bbox))

        assert pq.read_metadata(str(plain_parquet_with_bbox)).metadata is None
        assert pq.read_table(str(plain_parquet_with_bbox)).equals(before)

    def test_cli_exits_non_zero(self, plain_parquet_with_bbox):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add

        result = CliRunner().invoke(add, ["bbox-metadata", str(plain_parquet_with_bbox)])

        assert result.exit_code != 0
        assert "no GeoParquet metadata" in result.output

    def test_no_temp_file_is_left_behind(self, plain_parquet_with_bbox):
        with pytest.raises(GeoParquetError):
            add_bbox_metadata(str(plain_parquet_with_bbox))

        leftovers = list(plain_parquet_with_bbox.parent.glob("*.tmp"))
        assert leftovers == []
