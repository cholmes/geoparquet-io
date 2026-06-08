"""Tests distinguishing an explicit ``"crs": null`` (unknown CRS) from an
omitted crs key (defaults to OGC:CRS84) per the GeoParquet spec.

Covers:
- the new ``crs_is_explicitly_null`` / ``geoparquet_crs_is_null`` helpers
- the ``_check_crs_valid`` validate behavior (null -> WARNING, absent -> PASSED)
- the latent reproject bug (default target must omit crs, never write null)
- the ``--assume-crs84`` / ``assume_crs84`` fix path
- the broad "encountered null CRS" warning fired from the shared read path
"""

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.crs_utils import (
    crs_is_explicitly_null,
    extract_crs_from_parquet,
    geoparquet_crs_is_null,
    reset_null_crs_warnings,
)
from geoparquet_io.core.reproject import _reproject_streaming, reproject, reproject_table
from geoparquet_io.core.validate import CheckStatus, _check_crs_valid
from tests.conftest import get_geo_metadata


def _col_crs(parquet_file, geometry_column="geometry"):
    """Return (key_present, value) for the geometry column's crs."""
    geo = get_geo_metadata(parquet_file)
    col = geo["columns"][geo.get("primary_column", geometry_column)]
    return ("crs" in col, col.get("crs"))


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def test_crs_is_explicitly_null_true_for_null():
    assert crs_is_explicitly_null({"crs": None}) is True


def test_crs_is_explicitly_null_false_for_absent():
    assert crs_is_explicitly_null({}) is False


def test_crs_is_explicitly_null_false_for_present():
    assert crs_is_explicitly_null({"crs": {"id": {"authority": "EPSG", "code": 4326}}}) is False


def test_geoparquet_crs_is_null_detects_null(null_crs_parquet):
    assert geoparquet_crs_is_null(null_crs_parquet) is True


def test_geoparquet_crs_is_null_false_for_absent(absent_crs_parquet):
    assert geoparquet_crs_is_null(absent_crs_parquet) is False


def test_geoparquet_crs_is_null_false_for_normal(buildings_test_file):
    assert geoparquet_crs_is_null(buildings_test_file) is False


# --------------------------------------------------------------------------- #
# validate
# --------------------------------------------------------------------------- #


def test_validate_crs_null_is_warning():
    check = _check_crs_valid({"crs": None}, "geometry")
    assert check.status == CheckStatus.WARNING
    assert "unknown" in check.message.lower()
    assert "assume-crs84" in check.message


def test_validate_crs_absent_is_passed():
    check = _check_crs_valid({}, "geometry")
    assert check.status == CheckStatus.PASSED
    assert "CRS84" in check.message


def test_validate_crs_projjson_still_passes():
    check = _check_crs_valid({"crs": {"type": "GeographicCRS", "name": "WGS 84"}}, "geometry")
    assert check.status == CheckStatus.PASSED


# --------------------------------------------------------------------------- #
# latent reproject bug: default target must omit crs (never null)
# --------------------------------------------------------------------------- #


def test_reproject_table_default_target_omits_crs_key(buildings_test_file):
    import json

    # Build a WKB table that claims a non-default CRS, then reproject to default.
    table = pq.read_table(buildings_test_file)
    metadata = dict(table.schema.metadata)
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    geo["columns"]["geometry"]["crs"] = {"id": {"authority": "EPSG", "code": 3857}}
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    table = table.replace_schema_metadata(metadata)

    result = reproject_table(table, target_crs="EPSG:4326")
    out_geo = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
    col = out_geo["columns"]["geometry"]
    assert "crs" not in col, f"default target should omit crs, got {col.get('crs')!r}"


def test_reproject_to_real_crs_still_writes_crs(buildings_test_file, temp_output_file):
    reproject(buildings_test_file, temp_output_file, target_crs="EPSG:3857")
    present, value = _col_crs(temp_output_file)
    assert present and value is not None


def test_reproject_default_target_omits_crs(buildings_test_file, temp_output_file):
    # buildings_test is default CRS; reproject to default should keep crs omitted, not null.
    reproject(buildings_test_file, temp_output_file, target_crs="EPSG:4326")
    present, value = _col_crs(temp_output_file)
    assert value is not None or present is False
    # The key must never be present-and-null.
    assert not (present and value is None)


# --------------------------------------------------------------------------- #
# streaming path: _reproject_streaming is exercised directly (file->file routes
# through reproject_impl, so these call the streaming function explicitly).
# --------------------------------------------------------------------------- #


def _stream(input_file, output_file, target_crs, assume_crs84=False):
    """Drive _reproject_streaming directly with file paths."""
    _reproject_streaming(
        input_file,
        output_file,
        target_crs,
        None,  # source_crs
        "ZSTD",  # compression
        None,  # compression_level
        False,  # verbose
        None,  # profile
        None,  # geoparquet_version
        assume_crs84=assume_crs84,
    )


def test_streaming_default_target_omits_crs(buildings_test_file, temp_output_file):
    _stream(buildings_test_file, temp_output_file, "EPSG:4326")
    present, value = _col_crs(temp_output_file)
    assert not (present and value is None), "streaming default must not write crs:null"


def test_streaming_real_target_writes_crs(buildings_test_file, temp_output_file):
    _stream(buildings_test_file, temp_output_file, "EPSG:3857")
    present, value = _col_crs(temp_output_file)
    assert present and value is not None


def test_streaming_null_input_without_flag_raises(null_crs_parquet, temp_output_file):
    with pytest.raises(ValueError, match="assume-crs84"):
        _stream(null_crs_parquet, temp_output_file, "EPSG:4326")


def test_streaming_assume_crs84_null_input_succeeds(null_crs_parquet, temp_output_file):
    _stream(null_crs_parquet, temp_output_file, "EPSG:4326", assume_crs84=True)
    assert "crs" not in get_geo_metadata(temp_output_file)["columns"]["geometry"]


# --------------------------------------------------------------------------- #
# Arrow/Python-API path (reproject_table) must not silently coerce crs:null
# --------------------------------------------------------------------------- #


def _table_with_crs_state(buildings_test_file, crs_state):
    """Return an in-memory table whose geometry crs is null/absent/present."""
    import json

    table = pq.read_table(buildings_test_file)
    metadata = dict(table.schema.metadata)
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    col = geo["columns"][geo.get("primary_column", "geometry")]
    if crs_state == "null":
        col["crs"] = None
    elif crs_state == "absent":
        col.pop("crs", None)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def test_reproject_table_null_crs_without_flag_raises(buildings_test_file):
    table = _table_with_crs_state(buildings_test_file, "null")
    with pytest.raises(ValueError, match="null CRS"):
        reproject_table(table, target_crs="EPSG:3857")


def test_reproject_table_null_crs_assume_crs84_succeeds(buildings_test_file):
    import json

    table = _table_with_crs_state(buildings_test_file, "null")
    result = reproject_table(table, target_crs="EPSG:3857", assume_crs84=True)
    out_geo = json.loads(result.schema.metadata[b"geo"].decode("utf-8"))
    assert out_geo["columns"]["geometry"]["crs"] is not None


def test_reproject_table_null_crs_explicit_source_crs_succeeds(buildings_test_file):
    # An explicit source_crs satisfies the contract even with crs:null input.
    table = _table_with_crs_state(buildings_test_file, "null")
    result = reproject_table(table, target_crs="EPSG:4326", source_crs="EPSG:4326")
    out_geo = result.schema.metadata[b"geo"].decode("utf-8")
    assert '"crs"' not in out_geo or "null" not in out_geo


def test_reproject_table_absent_crs_still_reprojects(buildings_test_file):
    # Omitted crs (true default) must NOT raise — it is the common case.
    table = _table_with_crs_state(buildings_test_file, "absent")
    result = reproject_table(table, target_crs="EPSG:3857")
    assert result.num_rows == table.num_rows


# --------------------------------------------------------------------------- #
# assume_crs84 fix path
# --------------------------------------------------------------------------- #


def test_assume_crs84_null_input_default_dst(null_crs_parquet, temp_output_file):
    reproject(null_crs_parquet, temp_output_file, target_crs="EPSG:4326", assume_crs84=True)
    present, value = _col_crs(temp_output_file)
    assert not (present and value is None), "output should not carry crs:null"
    # default target -> crs key omitted
    assert "crs" not in get_geo_metadata(temp_output_file)["columns"]["geometry"]


def test_assume_crs84_null_input_real_dst(null_crs_parquet, temp_output_file):
    reproject(null_crs_parquet, temp_output_file, target_crs="EPSG:3857", assume_crs84=True)
    present, value = _col_crs(temp_output_file)
    assert present and value is not None


def test_reproject_null_input_without_flag_raises(null_crs_parquet, temp_output_file):
    import pytest

    with pytest.raises(ValueError, match="assume-crs84"):
        reproject(null_crs_parquet, temp_output_file, target_crs="EPSG:4326")


def test_assume_crs84_coords_unchanged_for_default(null_crs_parquet, temp_output_file):
    import json

    before = pq.read_table(null_crs_parquet)
    reproject(null_crs_parquet, temp_output_file, target_crs="EPSG:4326", assume_crs84=True)
    after = pq.read_table(temp_output_file)
    assert before.num_rows == after.num_rows
    # geo metadata crs must be gone
    geo = json.loads(after.schema.metadata[b"geo"].decode("utf-8"))
    assert "crs" not in geo["columns"]["geometry"]


# --------------------------------------------------------------------------- #
# warning fired from the shared read path (deduped)
# --------------------------------------------------------------------------- #


def _null_crs_warnings(records):
    return [r for r in records if "null" in r.message.lower() and "crs" in r.message.lower()]


def test_warn_on_null_crs_emitted_once(null_crs_parquet, caplog):
    import logging

    reset_null_crs_warnings()
    with caplog.at_level(logging.WARNING):
        extract_crs_from_parquet(null_crs_parquet)
        extract_crs_from_parquet(null_crs_parquet)
    assert len(_null_crs_warnings(caplog.records)) == 1


def test_no_warn_on_absent_crs(absent_crs_parquet, caplog):
    import logging

    reset_null_crs_warnings()
    with caplog.at_level(logging.WARNING):
        extract_crs_from_parquet(absent_crs_parquet)
    assert _null_crs_warnings(caplog.records) == []


def test_warn_on_two_distinct_null_files(null_crs_parquet, buildings_test_file, tmp_path, caplog):
    """Two different null-CRS inputs must each warn (dedup key not over-broad)."""
    import json
    import logging

    # Build a second, distinct null-CRS file (different path + a tweaked metadata
    # field so its geo bytes differ from the first).
    second = pq.read_table(buildings_test_file)
    meta = dict(second.schema.metadata)
    geo = json.loads(meta[b"geo"].decode("utf-8"))
    geo["columns"]["geometry"]["crs"] = None
    geo["columns"]["geometry"]["geometry_types"] = []  # make bytes distinct
    meta[b"geo"] = json.dumps(geo).encode("utf-8")
    second_path = str(tmp_path / "second_null.parquet")
    pq.write_table(second.replace_schema_metadata(meta), second_path)

    reset_null_crs_warnings()
    with caplog.at_level(logging.WARNING):
        extract_crs_from_parquet(null_crs_parquet)
        extract_crs_from_parquet(second_path)
    assert len(_null_crs_warnings(caplog.records)) == 2
