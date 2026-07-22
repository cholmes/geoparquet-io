"""Unit tests for verified code-review fixes (todos 036, 038, 039, 042).

Covers:
- 036: _crs_equals must not report equality for arbitrary id-less PROJJSON pairs
- 038: validator must not crash on malformed metadata (numeric crs.id authority,
       valid-JSON non-object geo values)
- 039: non-planar edges declared on a planar GEOMETRY column must be flagged
       in 2.0 (WARNING — see TestEdgesOnPlanarGeometry for the adjudication)
- 042: epoch check must distinguish explicit-null CRS, and never silently PASS
       when the CRS datum cannot be resolved
"""

import json
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyproj import CRS as PyprojCRS

from geoparquet_io.core.validate import (
    CheckStatus,
    _check_epoch_valid,
    _check_v2_edges_consistency,
    _crs_equals,
    _is_ogc_crs84,
    validate_geoparquet,
)

WKB_POINT = struct.pack("<BIdd", 1, 1, 30.0, 10.0)


def _idless_projjson(user_input) -> dict:
    crs = PyprojCRS.from_user_input(user_input).to_json_dict()
    crs.pop("id", None)
    return crs


class TestCrsEqualsIdless:
    """036: id-less PROJJSON dicts must be compared semantically, not assumed equal."""

    def test_idless_3857_vs_idless_utm32n_not_equal(self):
        assert not _crs_equals(_idless_projjson("EPSG:3857"), _idless_projjson("EPSG:32632"))

    def test_identical_idless_projjson_equal(self):
        a = _idless_projjson("EPSG:3857")
        b = json.loads(json.dumps(a))
        assert _crs_equals(a, b)

    def test_both_with_matching_id_equal(self):
        a = {"type": "ProjectedCRS", "name": "A", "id": {"authority": "EPSG", "code": 3857}}
        b = {"type": "ProjectedCRS", "name": "B", "id": {"authority": "EPSG", "code": 3857}}
        assert _crs_equals(a, b)

    def test_both_with_different_id_not_equal(self):
        a = {"type": "ProjectedCRS", "name": "A", "id": {"authority": "EPSG", "code": 3857}}
        b = {"type": "ProjectedCRS", "name": "A", "id": {"authority": "EPSG", "code": 32632}}
        assert not _crs_equals(a, b)

    def test_different_dicts_sharing_name_are_not_equal(self):
        """The old name-only fallback made these equal; must fail closed now."""
        assert not _crs_equals(
            {"name": "whatever", "type": "GeographicCRS"},
            {"name": "whatever", "type": "ProjectedCRS"},
        )

    def test_identical_unparsable_dicts_are_equal(self):
        """Byte-identical declarations describe the same CRS even if unparsable."""
        assert _crs_equals({"name": "whatever"}, {"name": "whatever"})

    def test_string_forms_unchanged(self):
        assert _crs_equals("EPSG:4326", "EPSG:4326")
        assert not _crs_equals("EPSG:4326", "EPSG:3857")

    def test_none_handling_unchanged(self):
        assert _crs_equals(None, None)
        assert not _crs_equals(None, {"name": "x"})


class TestMalformedMetadataNoCrash:
    """038: malformed metadata must yield FAILED results, never exceptions."""

    def test_is_ogc_crs84_numeric_authority_no_crash(self):
        assert _is_ogc_crs84({"id": {"authority": 6, "code": "CRS84"}}) is False

    def test_is_ogc_crs84_non_dict_id_no_crash(self):
        assert _is_ogc_crs84({"id": "EPSG:4326"}) is False

    def _write_with_geo_bytes(self, tmp_path, geo_bytes):
        table = pa.table({"a": [1], "geometry": [WKB_POINT]})
        table = table.replace_schema_metadata({b"geo": geo_bytes})
        out = tmp_path / "bad.parquet"
        pq.write_table(table, out)
        return out

    def test_numeric_authority_in_crs_id_reports_failed(self, tmp_path):
        geo = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": [],
                    "crs": {"id": {"authority": 1234, "code": 5678}, "name": "weird"},
                }
            },
        }
        out = self._write_with_geo_bytes(tmp_path, json.dumps(geo).encode())
        result = validate_geoparquet(str(out), validate_data=True)
        assert not result.is_valid
        assert any(c.status == CheckStatus.FAILED for c in result.checks)

    @pytest.mark.parametrize("geo_bytes", [b'["1.0.0"]', b'"hello"'])
    def test_non_object_geo_json_reports_failed_auto(self, tmp_path, geo_bytes):
        out = self._write_with_geo_bytes(tmp_path, geo_bytes)
        result = validate_geoparquet(str(out), validate_data=False)
        assert not result.is_valid
        failed = [c.name for c in result.checks if c.status == CheckStatus.FAILED]
        assert any("geo_metadata" in n for n in failed), failed

    @pytest.mark.parametrize("target", ["1.0", "1.1", "2.0"])
    def test_non_object_geo_json_reports_failed_explicit_version(self, tmp_path, target):
        out = self._write_with_geo_bytes(tmp_path, b'["1.0.0"]')
        result = validate_geoparquet(str(out), target_version=target, validate_data=False)
        assert not result.is_valid
        assert any(c.status == CheckStatus.FAILED for c in result.checks)


GEOMETRY_SCHEMA = [{"name": "geometry", "logical_type": "GeometryType()", "type": "binary"}]
GEOGRAPHY_SCHEMA = [{"name": "geometry", "logical_type": "GeographyType()", "type": "binary"}]
WKB_SCHEMA = [{"name": "geometry", "logical_type": "", "type": "binary"}]


def _edges_meta(edges=None):
    col: dict = {"encoding": "WKB"}
    if edges is not None:
        col["edges"] = edges
    return {"columns": {"geometry": col}}


class TestEdgesOnPlanarGeometry:
    """039: non-planar edges claims on a planar GEOMETRY column must be flagged in 2.0.

    Flagged as WARNING, not FAILED: the official 2.0.0 metadata schema permits
    any edges value regardless of logical type, the geoparquet-testing corpus
    ships data/edges/edges-{vincenty,thomas,andoyer,karney}.parquet as valid
    files with planar GEOMETRY logical types, and gpio's own convert emits this
    shape when demoting GEOGRAPHY (#588). The review's concern — the claim was
    never surfaced at all — is met by the warning.
    """

    @pytest.mark.parametrize("edges", ["vincenty", "spherical", "thomas", "andoyer", "karney"])
    def test_non_planar_edges_on_geometry_warns(self, edges):
        check = _check_v2_edges_consistency(_edges_meta(edges), GEOMETRY_SCHEMA, "geometry")
        assert check.status == CheckStatus.WARNING, check.message
        assert edges in check.message

    @pytest.mark.parametrize("edges", [None, "planar"])
    def test_planar_or_absent_edges_on_geometry_not_flagged(self, edges):
        check = _check_v2_edges_consistency(_edges_meta(edges), GEOMETRY_SCHEMA, "geometry")
        assert check.status == CheckStatus.SKIPPED, check.message

    def test_wkb_column_still_skipped(self):
        """1.x-style WKB columns (no native logical type) keep the SKIP behavior."""
        check = _check_v2_edges_consistency(_edges_meta("spherical"), WKB_SCHEMA, "geometry")
        assert check.status == CheckStatus.SKIPPED

    def test_geography_matching_edges_still_passes(self):
        check = _check_v2_edges_consistency(_edges_meta("spherical"), GEOGRAPHY_SCHEMA, "geometry")
        assert check.status == CheckStatus.PASSED, check.message

    def test_geography_mismatched_edges_still_fails(self):
        check = _check_v2_edges_consistency(_edges_meta("vincenty"), GEOGRAPHY_SCHEMA, "geometry")
        assert check.status == CheckStatus.FAILED


class TestEpochCrsHandling:
    """042: explicit-null CRS and unresolvable CRSs must surface as WARNING."""

    def test_explicit_null_crs_with_epoch_warns(self):
        check = _check_epoch_valid({"epoch": 2020.0, "crs": None}, "geometry")
        assert check.status == CheckStatus.WARNING, check.message

    def test_absent_crs_with_epoch_fails(self):
        """Absent crs means the OGC:CRS84 default — a datum ensemble."""
        check = _check_epoch_valid({"epoch": 2020.0}, "geometry")
        assert check.status == CheckStatus.FAILED

    def test_unresolvable_crs_with_epoch_warns_not_passes(self):
        crs = {
            "type": "EngineeringCRS",
            "name": "Site grid",
            "id": {"authority": "EPSG", "code": 5817},
        }
        check = _check_epoch_valid({"epoch": 2020.0, "crs": crs}, "geometry")
        assert check.status == CheckStatus.WARNING, check.message

    def test_garbage_crs_with_epoch_warns_not_passes(self):
        crs = {"id": {"authority": "XYZ", "code": 1}}
        check = _check_epoch_valid({"epoch": 2020.0, "crs": crs}, "geometry")
        assert check.status == CheckStatus.WARNING, check.message
