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
