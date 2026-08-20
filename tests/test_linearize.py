"""Tests for pure-Python curved-WKB linearization (issue #643, tier 3)."""

import math
import struct
from pathlib import Path

import pytest

from geoparquet_io.core.linearize import (
    LinearizeError,
    contains_curved_wkb,
    linearize_wkb,
)

TEST_DATA_DIR = Path(__file__).parent / "data"
CURVED_GPKG = TEST_DATA_DIR / "curved_geometry_test.gpkg"
LINEAR_GPKG = TEST_DATA_DIR / "buildings_test.gpkg"


def _wkb(type_code: int, body: bytes) -> bytes:
    return b"\x01" + struct.pack("<I", type_code) + body


def _points(*pts: tuple) -> bytes:
    out = struct.pack("<I", len(pts))
    for pt in pts:
        out += struct.pack("<" + "d" * len(pt), *pt)
    return out


def _parse_linestring(wkb: bytes, dims: int = 2) -> list[tuple]:
    assert wkb[0] == 1
    (code,) = struct.unpack_from("<I", wkb, 1)
    (n,) = struct.unpack_from("<I", wkb, 5)
    vals = struct.unpack_from("<" + "d" * (n * dims), wkb, 9)
    return code, [tuple(vals[i * dims : (i + 1) * dims]) for i in range(n)]


def _parse_polygon(wkb: bytes, dims: int = 2) -> tuple[int, list[list[tuple]]]:
    assert wkb[0] == 1
    (code,) = struct.unpack_from("<I", wkb, 1)
    (n_rings,) = struct.unpack_from("<I", wkb, 5)
    pos = 9
    rings = []
    for _ in range(n_rings):
        (n,) = struct.unpack_from("<I", wkb, pos)
        pos += 4
        vals = struct.unpack_from("<" + "d" * (n * dims), wkb, pos)
        pos += 8 * n * dims
        rings.append([tuple(vals[i * dims : (i + 1) * dims]) for i in range(n)])
    return code, rings


def _shoelace(pts: list[tuple]) -> float:
    area = 0.0
    for (x1, y1), (x2, y2) in zip(pts, pts[1:] + pts[:1], strict=True):
        area += x1 * y2 - x2 * y1
    return area / 2.0


class TestStroking:
    def test_half_circle_arc(self):
        """CIRCULARSTRING(0 0, 1 1, 2 0): unit half-circle centred at (1, 0)."""
        src = _wkb(8, _points((0, 0), (1, 1), (2, 0)))
        out, changed = linearize_wkb(src)

        assert changed
        code, pts = _parse_linestring(out)
        assert code == 2  # LINESTRING
        assert pts[0] == (0.0, 0.0) and pts[-1] == (2.0, 0.0)  # exact endpoints
        assert len(pts) >= 40  # ~180 degrees at 4 degrees/segment
        for x, y in pts:
            assert math.hypot(x - 1.0, y - 0.0) == pytest.approx(1.0, abs=1e-9)

    def test_z_values_interpolate(self):
        src = _wkb(1008, _points((0, 0, 0), (1, 1, 5), (2, 0, 10)))
        out, changed = linearize_wkb(src)

        assert changed
        code, pts = _parse_linestring(out, dims=3)
        assert code == 1002  # LINESTRING Z
        zs = [p[2] for p in pts]
        assert zs[0] == 0.0 and zs[-1] == 10.0
        assert zs == sorted(zs)  # monotone along the sweep

    def test_collinear_arc_degrades_to_segments(self):
        src = _wkb(8, _points((0, 0), (1, 0), (2, 0)))
        out, changed = linearize_wkb(src)

        assert changed
        _, pts = _parse_linestring(out)
        assert pts == [(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)]

    def test_compoundcurve_joins_segments(self):
        line = _wkb(2, _points((0, 0), (1, 0)))
        arc = _wkb(8, _points((1, 0), (2, 1), (3, 0)))
        src = _wkb(9, struct.pack("<I", 2) + line + arc)
        out, changed = linearize_wkb(src)

        assert changed
        code, pts = _parse_linestring(out)
        assert code == 2
        assert pts[0] == (0.0, 0.0) and pts[-1] == (3.0, 0.0)
        assert pts.count((1.0, 0.0)) == 1  # joint vertex not duplicated

    def test_multisurface_becomes_multipolygon(self):
        ring = _wkb(8, _points((0, 0), (1, 1), (2, 0), (1, -1), (0, 0)))
        curvepoly = _wkb(10, struct.pack("<I", 1) + ring)
        src = _wkb(12, struct.pack("<I", 1) + curvepoly)
        out, changed = linearize_wkb(src)

        assert changed
        assert out[0] == 1
        (code,) = struct.unpack_from("<I", out, 1)
        assert code == 6  # MULTIPOLYGON


class TestClosedCircle:
    def test_three_point_circle_full_sweep(self):
        """CIRCULARSTRING(0 0, 2 0, 0 0): the compact full-circle encoding.

        The circumcenter determinant is identically zero when p2 == p0, so
        without the dedicated branch this collapsed to a zero-area line.
        """
        src = _wkb(8, _points((0, 0), (2, 0), (0, 0)))
        out, changed = linearize_wkb(src)

        assert changed
        code, pts = _parse_linestring(out)
        assert code == 2
        assert len(pts) >= 90  # 360 degrees at 4 degrees/segment
        assert pts[0] == (0.0, 0.0) and pts[-1] == (0.0, 0.0)
        for x, y in pts:
            assert math.hypot(x - 1.0, y) == pytest.approx(1.0, abs=1e-9)

    def test_curvepolygon_circle_has_area(self):
        ring = _wkb(8, _points((0, 0), (2, 0), (0, 0)))
        src = _wkb(10, struct.pack("<I", 1) + ring)
        out, changed = linearize_wkb(src)

        assert changed
        code, rings = _parse_polygon(out)
        assert code == 3  # POLYGON
        assert rings[0][0] == rings[0][-1]  # ring closed
        assert abs(_shoelace(rings[0][:-1])) == pytest.approx(math.pi, rel=0.005)

    def test_closed_subarc_mid_chain(self):
        """pts[i] == pts[i+2] inside a longer chain must not collapse either."""
        src = _wkb(8, _points((1, 0), (-1, 0), (1, 0), (0, -1), (-1, 0)))
        out, changed = linearize_wkb(src)

        assert changed
        _, pts = _parse_linestring(out)
        assert len(pts) >= 130  # full circle plus a half circle
        for x, y in pts:
            assert math.hypot(x, y) == pytest.approx(1.0, abs=1e-9)


class TestSweepAndDims:
    def test_counterclockwise_arc(self):
        """CIRCULARSTRING(2 0, 1 1, 0 0): the same half-circle swept CCW."""
        src = _wkb(8, _points((2, 0), (1, 1), (0, 0)))
        out, changed = linearize_wkb(src)

        assert changed
        _, pts = _parse_linestring(out)
        assert pts[0] == (2.0, 0.0) and pts[-1] == (0.0, 0.0)
        for x, y in pts:
            assert math.hypot(x - 1.0, y) == pytest.approx(1.0, abs=1e-9)
        assert all(y >= 0 for _, y in pts)  # stays on the upper half

    def test_m_values_interpolate(self):
        src = _wkb(2008, _points((0, 0, 0), (1, 1, 5), (2, 0, 10)))
        out, changed = linearize_wkb(src)

        assert changed
        code, pts = _parse_linestring(out, dims=3)
        assert code == 2002  # LINESTRING M
        ms = [p[2] for p in pts]
        assert ms[0] == 0.0 and ms[-1] == 10.0

    def test_zm_values_interpolate(self):
        src = _wkb(3008, _points((0, 0, 0, 0), (1, 1, 5, 50), (2, 0, 10, 100)))
        out, changed = linearize_wkb(src)

        assert changed
        code, pts = _parse_linestring(out, dims=4)
        assert code == 3002  # LINESTRING ZM
        assert pts[-1] == (2.0, 0.0, 10.0, 100.0)

    def test_unrecognized_band_rejected(self):
        src = _wkb(4008, _points((0, 0), (1, 1), (2, 0)))
        with pytest.raises(LinearizeError, match="Unrecognized"):
            linearize_wkb(src)

    def test_mixed_zm_inside_curve_rejected(self):
        ring = _wkb(1008, _points((0, 0, 0), (1, 1, 5), (2, 0, 10)))  # Z ring
        src = _wkb(10, struct.pack("<I", 1) + ring)  # 2D CURVEPOLYGON
        with pytest.raises(LinearizeError, match="Mixed Z/M"):
            linearize_wkb(src)


class TestMoreShapes:
    def test_geometrycollection_children_linearized(self):
        arc = _wkb(8, _points((0, 0), (1, 1), (2, 0)))
        src = _wkb(7, struct.pack("<I", 1) + arc)
        out, changed = linearize_wkb(src)

        assert changed
        (code,) = struct.unpack_from("<I", out, 1)
        assert code == 7  # still a GEOMETRYCOLLECTION
        (child_code,) = struct.unpack_from("<I", out, 10)
        assert child_code == 2  # child is now a LINESTRING

    def test_point_and_polygon_passthrough(self):
        point = _wkb(1, struct.pack("<dd", 3.0, 4.0))
        ring = _points((0, 0), (1, 0), (1, 1), (0, 0))
        polygon = _wkb(3, struct.pack("<I", 1) + ring)
        multipolygon = _wkb(6, struct.pack("<I", 1) + polygon)

        for src in (point, polygon, multipolygon):
            out, changed = linearize_wkb(src)
            assert not changed
            assert out == src

    def test_compoundcurve_ring_inside_curvepolygon(self):
        """A COMPOUNDCURVE used as a CURVEPOLYGON ring is linearized too."""
        line = _wkb(2, _points((0, 0), (2, 0)))
        arc = _wkb(8, _points((2, 0), (1, 1), (0, 0)))
        compound = _wkb(9, struct.pack("<I", 2) + line + arc)
        src = _wkb(10, struct.pack("<I", 1) + compound)
        out, changed = linearize_wkb(src)

        assert changed
        code, rings = _parse_polygon(out)
        assert code == 3
        assert rings[0][0] == rings[0][-1]
        assert abs(_shoelace(rings[0][:-1])) == pytest.approx(math.pi / 2, rel=0.005)

    def test_open_arc_ring_gets_closed(self):
        """A CURVEPOLYGON ring that ends away from its start is closed."""
        ring = _wkb(8, _points((0, 0), (1, 1), (2, 0)))
        src = _wkb(10, struct.pack("<I", 1) + ring)
        out, changed = linearize_wkb(src)

        assert changed
        _, rings = _parse_polygon(out)
        assert rings[0][0] == rings[0][-1]

    def test_big_endian_input_normalized(self):
        src = (
            b"\x00"
            + struct.pack(">I", 2)
            + struct.pack(">I", 2)
            + struct.pack(">dddd", 0.0, 0.0, 5.0, 5.0)
        )
        out, changed = linearize_wkb(src)

        assert not changed
        assert out == _wkb(2, _points((0, 0), (5, 5)))  # little-endian re-emission


class TestDegenerateArcs:
    def test_duplicate_control_points_emit_no_duplicate_vertices(self):
        src = _wkb(8, _points((0, 0), (0, 0), (2, 0)))
        out, changed = linearize_wkb(src)

        assert changed
        _, pts = _parse_linestring(out)
        assert pts == [(0.0, 0.0), (2.0, 0.0)]

    def test_all_coincident_points(self):
        src = _wkb(8, _points((1, 1), (1, 1), (1, 1)))
        out, changed = linearize_wkb(src)

        assert changed
        _, pts = _parse_linestring(out)
        assert set(pts) == {(1.0, 1.0)}


class TestMaxAngleValidation:
    @pytest.mark.parametrize("bad", [0, -4.0, float("nan"), None])
    def test_non_positive_tolerance_rejected(self, bad):
        src = _wkb(8, _points((0, 0), (1, 1), (2, 0)))
        with pytest.raises(ValueError, match="max_angle_deg"):
            linearize_wkb(src, bad)


class TestPassthrough:
    def test_linear_geometry_unchanged(self):
        src = _wkb(2, _points((0, 0), (5, 5)))
        out, changed = linearize_wkb(src)
        assert not changed
        assert out == src  # already little-endian: byte-identical


class TestErrors:
    def test_surface_family_rejected(self):
        src = _wkb(16, struct.pack("<I", 0))  # TIN
        with pytest.raises(LinearizeError, match="cannot be linearized"):
            linearize_wkb(src)

    def test_ewkb_flags_rejected(self):
        src = b"\x01" + struct.pack("<I", 0x20000008) + _points((0, 0), (1, 1), (2, 0))
        with pytest.raises(LinearizeError, match="EWKB"):
            linearize_wkb(src)

    def test_even_point_count_rejected(self):
        src = _wkb(8, _points((0, 0), (1, 1)))
        with pytest.raises(LinearizeError, match="odd point count"):
            linearize_wkb(src)

    def test_non_curve_ring_component_rejected(self):
        point = _wkb(1, struct.pack("<dd", 0.0, 0.0))
        src = _wkb(10, struct.pack("<I", 1) + point)  # POINT as a ring
        with pytest.raises(LinearizeError, match="curve component"):
            linearize_wkb(src)


class TestContainsCurved:
    def test_detects_curved_and_linear(self):
        assert contains_curved_wkb(_wkb(10, b""))
        assert not contains_curved_wkb(_wkb(3, b""))


class TestConvertIntegration:
    def test_convert_linearizes_curved_gpkg(self, tmp_path):
        """The curved fixture that used to be a hard error now converts (#643)."""
        import duckdb

        import geoparquet_io as gpio

        out = tmp_path / "linear.parquet"
        gpio.convert(str(CURVED_GPKG)).write(str(out))

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        area, n = con.execute(
            f"SELECT ST_Area(geometry), ST_NPoints(geometry) FROM '{out}'"
        ).fetchone()
        # The fixture's CURVEPOLYGON is the full unit circle centred at (1, 0).
        assert area == pytest.approx(math.pi, rel=0.005)
        assert n >= 80

    def test_error_fallback_for_formats_without_prescan(self, monkeypatch, tmp_path):
        """Formats without a cheap curve scan (e.g. FileGDB) linearize via the
        DuckDB-error fallback instead of the GPKG pre-scan."""
        import duckdb

        import geoparquet_io as gpio
        import geoparquet_io.core.convert as convert_mod

        monkeypatch.setattr(convert_mod, "_choose_read_strategy", lambda *a, **k: "normal")
        out = tmp_path / "linear.parquet"
        gpio.convert(str(CURVED_GPKG)).write(str(out))

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        area = con.execute(f"SELECT ST_Area(geometry) FROM '{out}'").fetchone()[0]
        assert area == pytest.approx(math.pi, rel=0.005)

    def test_linearized_path_passes_linear_rows_through(self, monkeypatch, tmp_path):
        """Linear rows on the linearized path keep their bytes (prescreen)."""
        import geoparquet_io as gpio
        import geoparquet_io.core.convert as convert_mod

        if not LINEAR_GPKG.exists():
            pytest.skip("buildings_test.gpkg not available")
        monkeypatch.setattr(convert_mod, "_choose_read_strategy", lambda *a, **k: "linearized")
        out = tmp_path / "buildings.parquet"
        gpio.convert(str(LINEAR_GPKG)).write(str(out))
        assert out.exists()


class TestConvertParameters:
    def test_opt_out_raises_actionable_error(self):
        """linearize_curves=False keeps the strict behavior (#646's error)."""
        import geoparquet_io as gpio
        from geoparquet_io.core.duckdb_metadata import GeoParquetError

        with pytest.raises(GeoParquetError, match="CONVERT_TO_LINEAR"):
            gpio.convert(str(CURVED_GPKG), linearize_curves=False)

    def test_max_angle_controls_density(self, tmp_path):
        """A coarser tolerance yields fewer stroked vertices."""
        import duckdb

        import geoparquet_io as gpio

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        counts = {}
        for deg in (2.0, 30.0):
            out = tmp_path / f"deg{int(deg)}.parquet"
            gpio.convert(str(CURVED_GPKG), max_angle_deg=deg).write(str(out))
            counts[deg] = con.execute(f"SELECT ST_NPoints(geometry) FROM '{out}'").fetchone()[0]
        assert counts[2.0] > counts[30.0] >= 13

    def test_non_positive_max_angle_rejected_early(self):
        import geoparquet_io as gpio
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="max_angle_deg"):
            gpio.convert(str(CURVED_GPKG), max_angle_deg=-4.0)


class TestStReadExpr:
    def test_keep_wkb_layer_name_escaped(self):
        """The keep_wkb fallback must escape layer names like the normal path."""
        from geoparquet_io.core.convert import _build_st_read_expr

        expr = _build_st_read_expr("f.gpkg", "Côte d'Ivoire", keep_wkb=True)
        assert "keep_wkb := true" in expr
        assert "layer := 'Côte d''Ivoire'" in expr


class TestCliConvert:
    def test_cli_convert_linearizes_by_default(self, tmp_path):
        """`gpio convert` on curved input now works end to end (review on #647)."""
        import duckdb
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(cli, ["convert", str(CURVED_GPKG), str(out), "--verbose"])
        assert result.exit_code == 0, result.output
        assert out.exists()

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        # The CLI path preserves the source's geometry column name (#328).
        area = con.execute(f"SELECT ST_Area(geom) FROM '{out}'").fetchone()[0]
        assert area == pytest.approx(math.pi, rel=0.005)

    def test_cli_opt_out_raises_actionable_error(self, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli, ["convert", str(CURVED_GPKG), str(out), "--no-linearize-curves"]
        )
        assert result.exit_code != 0
        assert "CONVERT_TO_LINEAR" in result.output

    def test_cli_rejects_non_positive_max_angle(self, tmp_path):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        out = tmp_path / "out.parquet"
        result = CliRunner().invoke(
            cli, ["convert", str(CURVED_GPKG), str(out), "--max-angle-deg", "-4"]
        )
        assert result.exit_code != 0


class TestProjectedCurvedGpkg:
    def test_crs_survives_linearized_read(self, tmp_path):
        """Curved + projected: CRS detection must still work on the fallback path.

        (Whether the detected CRS then reaches the written file is #625/#644's
        territory; this pins the property that the linearized read does not
        bypass detection.)
        """
        import json
        import shutil
        import sqlite3

        from geoparquet_io.core.convert import read_spatial_to_arrow

        wkt_25830 = (
            'PROJCS["ETRS89 / UTM zone 30N",GEOGCS["ETRS89",'
            'DATUM["European_Terrestrial_Reference_System_1989",'
            'SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],'
            'AUTHORITY["EPSG","6258"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],'
            'UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],'
            'AUTHORITY["EPSG","4258"]],PROJECTION["Transverse_Mercator"],'
            'PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-3],'
            'PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],'
            'PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],'
            'AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","25830"]]'
        )
        gpkg = tmp_path / "curved_25830.gpkg"
        shutil.copy(CURVED_GPKG, gpkg)
        con = sqlite3.connect(gpkg)
        con.execute(
            "INSERT OR REPLACE INTO gpkg_spatial_ref_sys "
            "(srs_name, srs_id, organization, organization_coordsys_id, definition) "
            "VALUES ('ETRS89 / UTM zone 30N', 25830, 'EPSG', 25830, ?)",
            (wkt_25830,),
        )
        con.execute("UPDATE gpkg_contents SET srs_id = 25830")
        con.execute("UPDATE gpkg_geometry_columns SET srs_id = 25830")
        con.commit()
        con.close()

        table, detected_crs, geom_col = read_spatial_to_arrow(str(gpkg), verbose=True)

        assert geom_col == "geometry"
        assert table.num_rows == 1
        assert detected_crs is not None
        assert "25830" in json.dumps(detected_crs)
