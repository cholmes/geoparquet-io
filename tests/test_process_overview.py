"""Tests for `gpio process overview` (aggregate rollups to coarser levels)."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.admin_datasets import OvertureAdminDataset
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.overview import create_overviews
from geoparquet_io.core.process.overview.detect import detect_aggregate_file
from geoparquet_io.core.process.overview.run import overview_output_path

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _write_admin_region_aggregate(path, with_geometry=True, extra_column=False):
    """A tiny region-level `process aggregate admin` output.

    Rows: two US regions, one FR region, and an 'unassigned' bucket.
    """
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    extra_col = ", 'x' AS notes" if extra_column else ""
    geom_col = (
        ", CASE WHEN admin_code = 'unassigned' THEN NULL "
        "ELSE ST_Buffer(ST_Point(lon, lat), 0.5) END AS geometry"
        if with_geometry
        else ""
    )
    con.execute(
        f"""
        COPY (
            SELECT admin_code, admin_code AS admin_name, count, sum_area,
                   avg_height, min_year, max_year, count_barn, count_other
                   {extra_col}{geom_col}
            FROM (VALUES
                ('US-CA', 2, 10.0, 4.0, 1990, 2000, 1, 1, -120.0, 37.0),
                ('US-NV', 3, 30.0, 6.0, 1980, 1995, 2, 1, -116.0, 39.0),
                ('FR-IDF', 4, 8.0, 2.5, 2001, 2020, 0, 4, 2.3, 48.8),
                ('unassigned', 1, 1.0, 1.0, 1970, 1970, 0, 1, 0.0, 0.0)
            ) AS t(admin_code, count, sum_area, avg_height, min_year,
                   max_year, count_barn, count_other, lon, lat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _write_country_cache(path):
    """A fake Overture per-level country cache (US split in two rows, FR in one)."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT country, 'country' AS subtype,
                   ST_Buffer(ST_Point(lon, lat), 2.0) AS geometry
            FROM (VALUES
                ('US', -120.0, 37.0),
                ('US', -116.0, 39.0),
                ('FR', 2.3, 48.8)
            ) AS t(country, lon, lat)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.fixture
def fake_country_cache(tmp_path, monkeypatch):
    cache = tmp_path / "country_cache.parquet"
    _write_country_cache(cache)
    monkeypatch.setattr(
        OvertureAdminDataset,
        "get_source_for_level",
        lambda self, level, no_cache=False: str(cache),
    )
    return cache


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


class TestDetection:
    def test_no_cell_column_errors_with_hint(self, tmp_path):
        path = tmp_path / "plain.parquet"
        pq.write_table(pa.table({"id": [1, 2], "count": [3, 4]}), path)
        with pytest.raises(InvalidParameterError, match="cell-column"):
            detect_aggregate_file(str(path))

    def test_missing_count_column_errors(self, tmp_path):
        path = tmp_path / "no_count.parquet"
        pq.write_table(pa.table({"admin_code": ["US-CA"], "sum_area": [1.0]}), path)
        with pytest.raises(InvalidParameterError, match="count"):
            detect_aggregate_file(str(path))

    def test_admin_region_aggregate_detected(self, tmp_path):
        path = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(path)
        info = detect_aggregate_file(str(path))
        assert info.scheme == "admin"
        assert info.cell_column == "admin_code"
        assert info.base_level == "region"
        assert info.out_geometry == "polygon"
        roles = {c.name: c.func for c in info.rollup_columns}
        assert roles == {
            "sum_area": "sum",
            "avg_height": "avg",
            "min_year": "min",
            "max_year": "max",
            "count_barn": "sum",
            "count_other": "sum",
        }

    def test_admin_country_level_input_errors(self, tmp_path):
        path = tmp_path / "by_country.parquet"
        pq.write_table(
            pa.table({"admin_code": ["US", "FR", "unassigned"], "count": [1, 2, 3]}),
            path,
        )
        with pytest.raises(InvalidParameterError, match="country level"):
            detect_aggregate_file(str(path))

    def test_unclassifiable_columns_are_dropped(self, tmp_path):
        path = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(path, extra_column=True)
        info = detect_aggregate_file(str(path))
        assert "notes" in info.dropped_columns
        assert "notes" not in {c.name for c in info.rollup_columns}

    def test_no_geometry_infers_none(self, tmp_path):
        path = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(path, with_geometry=False)
        info = detect_aggregate_file(str(path))
        assert info.out_geometry == "none"


# ---------------------------------------------------------------------------
# Output naming
# ---------------------------------------------------------------------------


class TestOutputNaming:
    def test_grid_naming(self):
        assert overview_output_path("/x/cells.parquet", "a5", 7) == "/x/cells_r7.parquet"
        assert overview_output_path("/x/cells.parquet", "h3", 4) == "/x/cells_r4.parquet"

    def test_admin_naming(self):
        assert (
            overview_output_path("/x/by_region.parquet", "admin", "country")
            == "/x/by_region_country.parquet"
        )

    def test_output_dir_override(self):
        assert overview_output_path("/x/cells.parquet", "a5", 4, output_dir="/y") == (
            "/y/cells_r4.parquet"
        )


# ---------------------------------------------------------------------------
# Admin rollup (fast: fake country cache, no network)
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("fake_country_cache")
class TestAdminRollup:
    def test_region_to_country_rollup(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)

        results = create_overviews(str(src))
        assert results == [("country", str(tmp_path / "by_region_country.parquet"))]

        table = pq.read_table(results[0][1])
        rows = {code: i for i, code in enumerate(table.column("admin_code").to_pylist())}
        assert set(rows) == {"US", "FR", "unassigned"}

        def col(name, code):
            return table.column(name)[rows[code]].as_py()

        # Exact rollups.
        assert col("count", "US") == 5
        assert col("sum_area", "US") == pytest.approx(40.0)
        assert col("min_year", "US") == 1980
        assert col("max_year", "US") == 2000
        assert col("count_barn", "US") == 3
        assert col("count_other", "US") == 2
        # Count-weighted average: (2*4.0 + 3*6.0) / 5.
        assert col("avg_height", "US") == pytest.approx(5.2)
        assert col("count", "FR") == 4
        assert col("avg_height", "FR") == pytest.approx(2.5)
        # admin_name mirrors the country code at this level.
        assert col("admin_name", "US") == "US"
        # Unassigned bucket passes through with NULL geometry.
        assert col("count", "unassigned") == 1
        assert col("geometry", "unassigned") is None
        # Assigned countries got a (unioned) polygon.
        assert col("geometry", "US") is not None
        assert col("geometry", "FR") is not None

    def test_explicit_levels_country(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        results = create_overviews(str(src), levels="country")
        assert [lvl for lvl, _ in results] == ["country"]

    def test_invalid_admin_level_errors(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        _write_admin_region_aggregate(src)
        with pytest.raises(InvalidParameterError, match="country"):
            create_overviews(str(src), levels="province")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


class TestCli:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["process", "overview", "--help"])
        assert result.exit_code == 0
        for opt in ("--levels", "--max-tile-kb", "--bytes-per-cell", "--cell-column"):
            assert opt in result.output

    @pytest.mark.usefixtures("fake_country_cache")
    def test_cli_admin_rollup(self, tmp_path):
        src = tmp_path / "by_region.parquet"
        outdir = tmp_path / "out"
        outdir.mkdir()
        _write_admin_region_aggregate(src)
        runner = CliRunner()
        result = runner.invoke(cli, ["process", "overview", str(src), "--output-dir", str(outdir)])
        assert result.exit_code == 0, result.output
        assert (outdir / "by_region_country.parquet").exists()

    def test_cli_bad_input_errors_cleanly(self, tmp_path):
        path = tmp_path / "plain.parquet"
        pq.write_table(pa.table({"id": [1]}), path)
        runner = CliRunner()
        result = runner.invoke(cli, ["process", "overview", str(path)])
        assert result.exit_code != 0
        assert "cell-column" in result.output


# ---------------------------------------------------------------------------
# Grid gold rollups (slow: DuckDB community extensions)
# ---------------------------------------------------------------------------

_GOLD_POINTS = [
    # Clusters around three cities plus outliers, with crop + area attributes.
    (2.35, 48.85, "wheat", 4.0),
    (2.36, 48.86, "corn", 2.0),
    (2.37, 48.84, "wheat", 6.5),
    (13.40, 52.52, "corn", 1.5),
    (13.41, 52.53, "corn", 3.5),
    (13.39, 52.51, "soy", 9.0),
    (-3.70, 40.42, "wheat", 7.25),
    (-3.71, 40.41, "soy", 0.75),
    (30.0, -10.0, "wheat", 5.0),
    (100.5, 13.75, "soy", 8.0),
]

_GOLD_METRIC = "sum:area,avg:area,min:area,max:area"


def _write_points_geoparquet(path, rows):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _assert_rollup_matches_direct(rolled_path, direct_path, cell_column):
    rolled = pq.read_table(rolled_path)
    direct = pq.read_table(direct_path)
    assert rolled.num_rows == direct.num_rows

    def by_cell(table):
        cells = table.column(cell_column).to_pylist()
        return {
            c: {n: table.column(n)[i].as_py() for n in table.column_names}
            for i, c in enumerate(cells)
        }

    rolled_rows = by_cell(rolled)
    direct_rows = by_cell(direct)
    assert set(rolled_rows) == set(direct_rows)
    compare_cols = [
        n for n in direct.column_names if n.startswith(("count", "sum_", "avg_", "min_", "max_"))
    ]
    assert compare_cols  # sanity: the fixture exercised every rollup kind
    for cell, expect in direct_rows.items():
        got = rolled_rows[cell]
        for name in compare_cols:
            if name.startswith("avg_"):
                assert got[name] == pytest.approx(expect[name]), (cell, name)
            else:
                assert got[name] == expect[name], (cell, name)


@pytest.mark.slow
@pytest.mark.network
def test_gold_rollup_a5(tmp_path):
    """Rolling res-7 cells up to res 5 must equal aggregating raw data at res 5."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    direct = tmp_path / "direct_r5.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    kwargs = {"metric": _GOLD_METRIC, "breakdown": "crop", "out_geometry": "both"}
    aggregate_by_a5(str(src), str(base), resolution=7, **kwargs)
    aggregate_by_a5(str(src), str(direct), resolution=5, **kwargs)

    results = create_overviews(str(base), levels=[5])
    assert results == [(5, str(tmp_path / "cells_r5.parquet"))]
    _assert_rollup_matches_direct(results[0][1], str(direct), "a5_cell")


@pytest.mark.slow
@pytest.mark.network
def test_gold_rollup_h3(tmp_path):
    """H3 hexagons do not nest exactly, so a point near a cell edge can have a
    res-7 cell whose res-5 *ancestor* differs from the point's direct res-5
    cell. The rollup follows the true hierarchy (h3_cell_to_parent), so the
    gold expectation here is computed hierarchy-faithfully in SQL: key every
    raw point by parent-of-its-res-7-cell, then aggregate. This also proves
    the count-weighted avg equals the true mean when the metric has no NULLs.
    """
    from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    kwargs = {"metric": _GOLD_METRIC, "breakdown": "crop", "out_geometry": "both"}
    aggregate_by_h3(str(src), str(base), resolution=7, **kwargs)

    results = create_overviews(str(base), levels="5")
    assert results == [(5, str(tmp_path / "cells_r5.parquet"))]

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    con.execute("INSTALL h3 FROM community; LOAD h3")
    expected_rows = con.execute(
        f"""
        SELECT h3_cell_to_parent(
                   h3_latlng_to_cell_string(ST_Y(geometry), ST_X(geometry), 7), 5
               ) AS h3_cell,
               COUNT(*) AS count,
               SUM(area) AS sum_area,
               AVG(area) AS avg_area,
               MIN(area) AS min_area,
               MAX(area) AS max_area,
               COUNT(*) FILTER (WHERE crop = 'wheat') AS count_wheat,
               COUNT(*) FILTER (WHERE crop = 'corn') AS count_corn,
               COUNT(*) FILTER (WHERE crop = 'soy') AS count_soy
        FROM read_parquet('{src}')
        GROUP BY 1
        """
    ).fetchall()
    columns = [
        "h3_cell",
        "count",
        "sum_area",
        "avg_area",
        "min_area",
        "max_area",
        "count_wheat",
        "count_corn",
        "count_soy",
    ]
    expected = {row[0]: dict(zip(columns, row, strict=True)) for row in expected_rows}
    con.close()

    rolled = pq.read_table(results[0][1])
    assert rolled.num_rows == len(expected)
    cells = rolled.column("h3_cell").to_pylist()
    assert set(cells) == set(expected)
    for i, cell in enumerate(cells):
        for name in columns[1:]:
            got = rolled.column(name)[i].as_py()
            if name == "avg_area":
                assert got == pytest.approx(expected[cell][name]), (cell, name)
            else:
                assert got == expected[cell][name], (cell, name)


@pytest.mark.slow
@pytest.mark.network
def test_level_not_coarser_than_base_errors(tmp_path):
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(base), resolution=7)
    with pytest.raises(InvalidParameterError, match="coarser"):
        create_overviews(str(base), levels=[7])


@pytest.mark.slow
@pytest.mark.network
def test_mixed_resolution_input_errors(tmp_path):
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    a = tmp_path / "r6.parquet"
    b = tmp_path / "r7.parquet"
    mixed = tmp_path / "mixed.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(a), resolution=6)
    aggregate_by_a5(str(src), str(b), resolution=7)
    pq.write_table(
        pa.concat_tables([pq.read_table(a), pq.read_table(b)], promote_options="default"),
        mixed,
    )
    with pytest.raises(InvalidParameterError, match="[Mm]ixed"):
        create_overviews(str(mixed), levels=[4])


@pytest.mark.slow
@pytest.mark.network
def test_auto_level_selection_a5(tmp_path):
    """With an absurd bytes-per-cell nothing fits, so the coarsest level is built."""
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(base), resolution=7)

    results = create_overviews(str(base), max_tile_kb=1, bytes_per_cell=1e6)
    assert [lvl for lvl, _ in results] == [0]
    assert (tmp_path / "cells_r0.parquet").exists()


@pytest.mark.slow
@pytest.mark.network
def test_table_overview_api(tmp_path):
    from geoparquet_io.api.table import Table
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    base = tmp_path / "cells.parquet"
    _write_points_geoparquet(src, _GOLD_POINTS)
    aggregate_by_a5(str(src), str(base), resolution=7, metric="sum:area")

    result = Table(pq.read_table(base)).overview(5)
    assert "a5_cell" in result.column_names
    assert "count" in result.column_names
    assert "sum_area" in result.column_names
    base_count = sum(pq.read_table(base).column("count").to_pylist())
    assert sum(result.to_arrow().column("count").to_pylist()) == base_count
