"""A malformed carried ``geo`` block must not crash the write (#771).

The ``geo`` key on an input file is arbitrary JSON written by somebody else's
tool. gpio's writers read it and index into it -- ``geo["columns"][col]`` --
without checking that ``columns`` is the mapping-of-mappings the spec requires,
so a block whose ``columns`` is null, a list, a string, or a mapping to
non-objects aborted the write with a bare ``TypeError`` from three frames deep.

The decision recorded in #771: a malformed block is a property of the *input*,
not a caller error, so it is treated the way an absent block is -- the malformed
parts are dropped, fresh metadata is built from the table, and one warning names
what was wrong. Nothing raises.
"""

from __future__ import annotations

import json
import logging
import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import _apply_geoparquet_metadata
from geoparquet_io.core.geo_metadata import (
    reset_malformed_geo_warnings,
    sanitize_geo_metadata,
)

# One WKB point (1.0, 2.0), little-endian.
POINT_WKB = struct.pack("<BI2d", 1, 1, 1.0, 2.0)

WRITE_STRATEGIES = ["in-memory", "streaming", "disk-rewrite", "duckdb-kv"]

# (id, the value carried as geo["columns"], the substring the warning must name)
MALFORMED_COLUMNS = [
    ("null", None, "'columns'"),
    ("list", ["geometry"], "'columns'"),
    ("string", "geometry", "'columns'"),
    ("entry_not_an_object", {"geometry": "WKB"}, "'columns'"),
]


def _table_with_geo(geo_block) -> pa.Table:
    table = pa.table({"id": [1], "geometry": pa.array([POINT_WKB], type=pa.binary())})
    return table.replace_schema_metadata({b"geo": json.dumps(geo_block).encode("utf-8")})


def _malformed_table(columns) -> pa.Table:
    return _table_with_geo({"version": "1.1.0", "primary_column": "geometry", "columns": columns})


def _geo_of(table: pa.Table) -> dict:
    return json.loads(table.schema.metadata[b"geo"].decode("utf-8"))


def _malformed_warnings(records) -> list[str]:
    return [r.getMessage() for r in records if "malformed" in r.getMessage().lower()]


def _assert_fresh_and_valid(geo: dict) -> None:
    """The rebuilt block must be the one gpio would have written with no input block."""
    assert geo["primary_column"] == "geometry"
    assert isinstance(geo["columns"], dict)
    assert isinstance(geo["columns"]["geometry"], dict)
    assert geo["columns"]["geometry"]["encoding"]


# =============================================================================
# The sanitizer itself
# =============================================================================


class TestSanitizeGeoMetadata:
    @pytest.mark.parametrize(("case", "columns", "_hint"), MALFORMED_COLUMNS)
    def test_drops_malformed_columns(self, case, columns, _hint):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"version": "1.1.0", "primary_column": "geometry", "columns": columns}
        )
        assert cleaned is not None
        assert cleaned.get("columns", {}) == {}

    def test_keeps_the_good_entries_beside_a_bad_one(self):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata(
            {"columns": {"geometry": {"encoding": "WKB"}, "other": "WKB"}}
        )
        assert cleaned["columns"] == {"geometry": {"encoding": "WKB"}}

    def test_drops_a_non_string_primary_column(self):
        reset_malformed_geo_warnings()
        cleaned = sanitize_geo_metadata({"primary_column": 123, "columns": {}})
        assert "primary_column" not in cleaned

    def test_a_block_that_is_not_an_object_is_dropped_entirely(self):
        reset_malformed_geo_warnings()
        assert sanitize_geo_metadata(["geometry"]) is None
        reset_malformed_geo_warnings()
        assert sanitize_geo_metadata("geometry") is None

    def test_none_passes_through(self):
        assert sanitize_geo_metadata(None) is None

    def test_a_well_formed_block_is_returned_unchanged(self):
        block = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "bbox": [0, 0, 1, 1]}},
        }
        assert sanitize_geo_metadata(block) is block

    def test_does_not_mutate_the_caller_s_dict(self):
        reset_malformed_geo_warnings()
        block = {"primary_column": "geometry", "columns": None}
        sanitize_geo_metadata(block)
        assert block["columns"] is None

    def test_names_the_offending_key_and_its_json_type(self, caplog):
        reset_malformed_geo_warnings()
        with caplog.at_level(logging.WARNING):
            sanitize_geo_metadata({"columns": ["geometry"]})
        messages = _malformed_warnings(caplog.records)
        assert len(messages) == 1
        assert "'geo'" in messages[0]
        assert "'columns'" in messages[0]
        assert "array" in messages[0]

    def test_warns_once_per_distinct_problem(self, caplog):
        reset_malformed_geo_warnings()
        with caplog.at_level(logging.WARNING):
            sanitize_geo_metadata({"columns": None})
            sanitize_geo_metadata({"columns": None})
        assert len(_malformed_warnings(caplog.records)) == 1


# =============================================================================
# The helper the issue reproduces on
# =============================================================================


@pytest.mark.parametrize(("case", "columns", "hint"), MALFORMED_COLUMNS)
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_apply_metadata_survives_a_malformed_columns_block(case, columns, hint, version, caplog):
    reset_malformed_geo_warnings()
    table = _malformed_table(columns)

    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(
            table, "geometry", version, original_metadata=table.schema.metadata
        )

    _assert_fresh_and_valid(_geo_of(result))

    messages = _malformed_warnings(caplog.records)
    assert len(messages) == 1, messages
    assert "'geo'" in messages[0]
    assert hint in messages[0]


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_apply_metadata_rebuilds_a_non_string_primary_column(version):
    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {"version": "1.1.0", "primary_column": 123, "columns": {"geometry": {"encoding": "WKB"}}}
    )
    result = _apply_geoparquet_metadata(
        table, "geometry", version, original_metadata=table.schema.metadata
    )
    _assert_fresh_and_valid(_geo_of(result))


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_apply_metadata_survives_a_geo_block_that_is_not_an_object(version):
    reset_malformed_geo_warnings()
    table = _table_with_geo(["geometry"])
    result = _apply_geoparquet_metadata(
        table, "geometry", version, original_metadata=table.schema.metadata
    )
    _assert_fresh_and_valid(_geo_of(result))


def test_a_well_formed_block_still_carries_through(caplog):
    """Regression: the shape check must not disturb a valid input block."""
    reset_malformed_geo_warnings()
    table = _table_with_geo(
        {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "orientation": "counterclockwise"}},
        }
    )
    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(
            table, "geometry", "1.1", original_metadata=table.schema.metadata
        )

    geo = _geo_of(result)
    _assert_fresh_and_valid(geo)
    assert geo["columns"]["geometry"]["orientation"] == "counterclockwise"
    assert _malformed_warnings(caplog.records) == []


def test_an_absent_geo_block_still_works(caplog):
    reset_malformed_geo_warnings()
    table = pa.table({"id": [1], "geometry": pa.array([POINT_WKB], type=pa.binary())})
    with caplog.at_level(logging.WARNING):
        result = _apply_geoparquet_metadata(table, "geometry", "1.1", original_metadata=None)
    _assert_fresh_and_valid(_geo_of(result))
    assert _malformed_warnings(caplog.records) == []


# =============================================================================
# Every write-path reader of the raw block goes through the one shape check
# =============================================================================


@pytest.mark.parametrize(("case", "columns", "_hint"), MALFORMED_COLUMNS)
def test_strategy_base_reader_sanitizes(case, columns, _hint):
    """``write_strategies.base`` builds metadata through its own reader (#771)."""
    from geoparquet_io.core.write_strategies.base import build_geo_metadata

    reset_malformed_geo_warnings()
    raw = json.dumps({"primary_column": "geometry", "columns": columns}).encode("utf-8")
    geo = build_geo_metadata("geometry", "1.1", original_metadata={b"geo": raw})
    _assert_fresh_and_valid(geo)


@pytest.mark.parametrize(
    "metadata",
    [
        {b"geo": json.dumps({"columns": None}).encode("utf-8")},
        {b"geo": json.dumps({"columns": None})},
        {b"geo": {"columns": None}},
        {"geo": json.dumps({"columns": None})},
        {"geo": {"columns": None}},
    ],
    ids=["bytes_key_bytes", "bytes_key_str", "bytes_key_dict", "str_key_str", "str_key_dict"],
)
def test_strategy_base_reader_sanitizes_every_key_shape(metadata):
    """The shape check applies whichever way the ``geo`` key was handed over."""
    from geoparquet_io.core.write_strategies.base import _parse_existing_geo_metadata

    reset_malformed_geo_warnings()
    assert _parse_existing_geo_metadata(metadata) == {}


def test_parse_geo_metadata_quietly_delegates_to_the_shared_check():
    """``common._parse_geo_metadata_quietly`` must not keep a second copy of the check."""
    from geoparquet_io.core.common import _parse_geo_metadata_quietly

    reset_malformed_geo_warnings()
    raw = json.dumps({"primary_column": 1, "columns": ["geometry"]}).encode("utf-8")
    assert _parse_geo_metadata_quietly({b"geo": raw}) == {}


def test_extract_crs_from_table_survives_a_malformed_block():
    """``Table.write`` resolves the CRS before building metadata, through this reader."""
    from geoparquet_io.core.streaming import extract_crs_from_table

    reset_malformed_geo_warnings()
    assert extract_crs_from_table(_malformed_table({"geometry": "WKB"}), "geometry") is None
    assert extract_crs_from_table(_malformed_table(None), "geometry") is None


# =============================================================================
# The public API boundary -- every write strategy
# =============================================================================


@pytest.mark.parametrize("strategy", WRITE_STRATEGIES)
@pytest.mark.parametrize(("case", "columns", "_hint"), MALFORMED_COLUMNS)
def test_table_write_survives_a_malformed_columns_block(case, columns, _hint, strategy, tmp_path):
    from geoparquet_io.api import Table

    reset_malformed_geo_warnings()
    out = tmp_path / f"{case}_{strategy}.parquet"
    Table(_malformed_table(columns)).write(
        out, write_strategy=strategy, geoparquet_version="1.1", compression="SNAPPY"
    )

    written = pq.ParquetFile(out).schema_arrow.metadata
    _assert_fresh_and_valid(json.loads(written[b"geo"].decode("utf-8")))
