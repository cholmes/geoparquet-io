"""Tests for non-linear geometry detection and error reporting (issue #643)."""

import sqlite3
import struct
from pathlib import Path

import pytest

from geoparquet_io.core.curved_geometry import (
    find_non_linear_gpkg_types,
    unsupported_wkb_error_message,
)
from geoparquet_io.core.duckdb_metadata import GeoParquetError

TEST_DATA_DIR = Path(__file__).parent / "data"
CURVED_GPKG = TEST_DATA_DIR / "curved_geometry_test.gpkg"
LINEAR_GPKG = TEST_DATA_DIR / "buildings_test.gpkg"


class TestFindNonLinearGpkgTypes:
    def test_detects_curvepolygon(self):
        assert find_non_linear_gpkg_types(CURVED_GPKG) == ["CURVEPOLYGON"]

    def test_linear_gpkg_is_clean(self):
        if not LINEAR_GPKG.exists():
            pytest.skip("buildings_test.gpkg not available")
        assert find_non_linear_gpkg_types(LINEAR_GPKG) == []

    def test_unreadable_file_returns_empty(self, tmp_path):
        bogus = tmp_path / "not_a.gpkg"
        bogus.write_text("plain text")
        assert find_non_linear_gpkg_types(bogus) == []


def _gpkg_blob(wkb_type: int) -> bytes:
    """Minimal GPKG geometry blob: header (no envelope) + a bare WKB header."""
    header = b"GP\x00\x01" + b"\x00\x00\x00\x00"  # magic, version, flags, srs_id
    return header + b"\x01" + struct.pack("<I", wkb_type) + b"\x00\x00\x00\x00"


def _make_fake_gpkg(path: Path, table: str, blobs: list[bytes]) -> None:
    quoted = table.replace('"', '""')
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT)")
    con.execute("CREATE TABLE gpkg_geometry_columns (table_name TEXT, column_name TEXT)")
    con.execute(f'CREATE TABLE "{quoted}" (geom BLOB)')
    con.execute("INSERT INTO gpkg_contents VALUES (?, 'features')", (table,))
    con.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom')", (table,))
    con.executemany(f'INSERT INTO "{quoted}" VALUES (?)', [(b,) for b in blobs])
    con.commit()
    con.close()


class TestScanBounds:
    def test_table_name_with_quote_is_escaped(self, tmp_path):
        gpkg = tmp_path / "weird.gpkg"
        _make_fake_gpkg(gpkg, 'we"ird', [_gpkg_blob(10)])
        assert find_non_linear_gpkg_types(gpkg) == ["CURVEPOLYGON"]

    def test_scan_is_capped_per_table(self, tmp_path, monkeypatch):
        """A curve beyond the cap is missed — safe, since callers fall back on
        the DuckDB error anyway; the cap only bounds the diagnostic wait."""
        import geoparquet_io.core.curved_geometry as cg

        monkeypatch.setattr(cg, "_SCAN_CAP", 1)
        gpkg = tmp_path / "capped.gpkg"
        _make_fake_gpkg(gpkg, "t", [_gpkg_blob(3), _gpkg_blob(10)])  # linear first
        assert cg.find_non_linear_gpkg_types(gpkg) == []


class TestUnsupportedWkbError:
    def test_unlinearizable_input_raises_actionable_error(self, monkeypatch):
        """When linearization cannot handle the WKB (e.g. the surface family),
        the error still names the offending types and the remedy."""
        import geoparquet_io as gpio
        from geoparquet_io.core.linearize import LinearizeError

        def _fail(wkb, max_angle_deg=4.0):
            raise LinearizeError("WKB type 16 cannot be linearized")

        import geoparquet_io.core.linearize as linearize_mod

        monkeypatch.setattr(linearize_mod, "linearize_wkb", _fail)

        with pytest.raises(GeoParquetError) as exc:
            gpio.convert(str(CURVED_GPKG))
        message = str(exc.value)
        assert "CURVEPOLYGON" in message
        assert "CONVERT_TO_LINEAR" in message

    def test_message_for_non_gpkg_falls_back(self):
        message = unsupported_wkb_error_message(
            "data.fgb", None, "Invalid Input Error: Unsupported geometry type in WKB"
        )
        assert "non-linear geometries" in message
        assert "CONVERT_TO_LINEAR" in message
        assert "Unsupported geometry type in WKB" in message

    def test_envelope_and_large_payload_still_classified(self, tmp_path):
        """The header slice must cover the largest GPKG envelope (XYZM, 64 B).

        The scan reads only the leading header bytes rather than whole blobs, so
        a geometry carrying a full envelope and a megabyte of coordinates must
        still be classified from the slice alone.
        """
        flags = 4 << 1  # envelope indicator 4 -> 64-byte XYZM envelope
        header = b"GP\x00" + bytes([flags]) + struct.pack("<i", 0) + b"\x00" * 64
        blob = header + b"\x01" + struct.pack("<I", 10) + b"\x00" * 1_000_000
        gpkg = tmp_path / "enveloped.gpkg"
        _make_fake_gpkg(gpkg, "t", [blob])
        assert find_non_linear_gpkg_types(gpkg) == ["CURVEPOLYGON"]
