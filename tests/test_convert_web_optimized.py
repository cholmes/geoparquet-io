"""End-to-end tests for the --optimize-for web convert profile (Tasks 4 + 5).

Verifies that convert_to_geoparquet(..., optimize_for="web") forces GeoParquet
2.0 with native GeospatialStatistics, keeps a covering bbox column for viewport
pruning, and routes the write through the streaming strategy so per-row-group
bboxes and the page index are actually produced (not silently skipped by the
plain-COPY fast path).
"""

import duckdb
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.metadata_utils import has_parquet_native_geo_stats


def _make_native_geo_with_flat_bbox(path):
    """Write a native-geo (GeoParquet 2.0) parquet whose schema already carries
    flat top-level xmin/ymin/xmax/ymax columns, mimicking common DuckDB/CARTO
    exports. Used to prove the web profile does not duplicate the bbox.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""
        COPY (
          SELECT ST_Point(x, y) AS geom,
                 x AS xmin, y AS ymin, x AS xmax, y AS ymax,
                 id AS feature_id
          FROM (VALUES (0, 0.0, 0.0), (1, 10.0, 20.0), (2, 5.0, 5.0)) t(id, x, y)
        ) TO '{path}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2');
        """
    )
    con.close()


def test_web_convert_reuses_existing_flat_bbox_no_duplicate(tmp_path):
    """When the input already carries flat xmin/ymin/xmax/ymax columns, the web
    profile must fold them into the single covering bbox struct rather than
    appending a second copy (issue: +130MB duplicate bbox on CARTO-style files).
    """
    src = tmp_path / "flat_bbox_input.parquet"
    _make_native_geo_with_flat_bbox(str(src))

    out = tmp_path / "web.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, optimize_for="web", verbose=False)

    names = pq.ParquetFile(str(out)).schema_arrow.names
    # Covering struct present, non-bbox attribute preserved.
    assert "bbox" in names
    assert "feature_id" in names
    # The redundant flat top-level bbox columns must be gone (folded into bbox).
    assert "xmin" not in names
    assert "ymin" not in names
    assert "xmax" not in names
    assert "ymax" not in names


def test_non_web_convert_keeps_flat_bbox_columns(tmp_path):
    """The fold-in is gated on the web profile. A default (non-web) convert must
    never silently drop user columns named xmin/ymin/xmax/ymax.
    """
    src = tmp_path / "flat_bbox_input.parquet"
    _make_native_geo_with_flat_bbox(str(src))

    out = tmp_path / "plain.parquet"
    convert_to_geoparquet(
        str(src), str(out), skip_hilbert=True, geoparquet_version="1.1", verbose=False
    )

    names = pq.ParquetFile(str(out)).schema_arrow.names
    for col in ("xmin", "ymin", "xmax", "ymax"):
        assert col in names


def test_web_convert_keeps_partial_flat_bbox_columns(tmp_path):
    """Only a complete xmin/ymin/xmax/ymax set is treated as a bbox. A partial
    set (here xmin/ymin only) is real data and must be preserved, not dropped.
    """
    src = tmp_path / "partial_bbox_input.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        f"""
        COPY (
          SELECT ST_Point(x, y) AS geom, x AS xmin, y AS ymin, id AS feature_id
          FROM (VALUES (0, 0.0, 0.0), (1, 10.0, 20.0)) t(id, x, y)
        ) TO '{src}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2');
        """
    )
    con.close()

    out = tmp_path / "web_partial.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, optimize_for="web", verbose=False)

    names = pq.ParquetFile(str(out)).schema_arrow.names
    assert "bbox" in names  # struct still added
    assert "xmin" in names and "ymin" in names  # partial set preserved


def test_table_write_web_folds_existing_flat_bbox(tmp_path):
    """The in-memory Table.write(optimize_for="web") path folds pre-existing flat
    bbox columns into the covering struct too, matching the file-path convert.
    """
    import geoparquet_io as gpio

    src = tmp_path / "flat_bbox_input.parquet"
    _make_native_geo_with_flat_bbox(str(src))

    out = tmp_path / "table_web.parquet"
    gpio.read(str(src)).write(str(out), optimize_for="web")

    names = pq.ParquetFile(str(out)).schema_arrow.names
    assert "bbox" in names
    assert "feature_id" in names
    for col in ("xmin", "ymin", "xmax", "ymax"):
        assert col not in names


@pytest.mark.slow
def test_web_convert_has_native_stats_and_covering(buildings_test_file, tmp_path):
    out = tmp_path / "web.parquet"
    convert_to_geoparquet(
        buildings_test_file, str(out), skip_hilbert=False, optimize_for="web", verbose=False
    )

    # 1. Native GeospatialStatistics present (GeoParquet 2.0).
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True

    pf = pq.ParquetFile(str(out))
    names = pf.schema_arrow.names
    # 2. Covering bbox column present despite version 2.0.
    assert "bbox" in names
    # 3. Covering bbox leaves carry per-row-group stats + page index.
    rg0 = pf.metadata.row_group(0)
    leaf_names = [rg0.column(i).path_in_schema for i in range(rg0.num_columns)]
    bbox_leaf = leaf_names.index("bbox.xmin")
    assert rg0.column(bbox_leaf).has_column_index is True
    assert rg0.column(bbox_leaf).has_offset_index is True


@pytest.mark.slow
def test_web_convert_geojson_input_has_native_stats_and_covering(geojson_input, tmp_path):
    out = tmp_path / "web_geojson.parquet"
    convert_to_geoparquet(
        geojson_input, str(out), skip_hilbert=False, optimize_for="web", verbose=False
    )

    # 1. Native GeospatialStatistics present (GeoParquet 2.0) for a non-parquet
    #    (GDAL/ST_Read) input, covering the _convert_spatial_path branch.
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True

    pf = pq.ParquetFile(str(out))
    names = pf.schema_arrow.names
    # 2. Covering bbox column forced even at v2.0, proving force_include_bbox
    #    threaded through the non-parquet path too.
    assert "bbox" in names


@pytest.mark.slow
def test_web_convert_row_group_bboxes_usable_after_hilbert(buildings_test_file, tmp_path):
    from geoparquet_io.core.duckdb_metadata import get_per_row_group_bbox_stats

    out = tmp_path / "web2.parquet"
    convert_to_geoparquet(
        buildings_test_file, str(out), skip_hilbert=False, optimize_for="web", verbose=False
    )
    stats = get_per_row_group_bbox_stats(str(out), bbox_column="bbox")
    assert len(stats) >= 1
    for s in stats:  # each row group has a finite bbox usable by a viewer
        assert s["xmin"] <= s["xmax"] and s["ymin"] <= s["ymax"]


@pytest.mark.slow
def test_cli_optimize_for_web(buildings_test_file, tmp_path):
    from click.testing import CliRunner

    from geoparquet_io.cli.main import convert

    out = tmp_path / "cli_web.parquet"
    result = CliRunner().invoke(
        convert, ["geoparquet", buildings_test_file, str(out), "--optimize-for", "web"]
    )
    assert result.exit_code == 0, result.output
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True
