"""Tests for Sort-Tile-Recursive spatial ordering."""

from __future__ import annotations

import json
import io
import struct
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.str_order import _str_layout, str_order, str_order_table


def _point_wkb(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def _empty_collection_wkb() -> bytes:
    return struct.pack("<BII", 1, 7, 0)


def _point_table(coords: list[tuple[int, float, float]]) -> pa.Table:
    """Build a metadata-bearing WKB point table from ``(id, x, y)`` rows."""
    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        ).encode()
    }
    return pa.table(
        {
            "id": pa.array([row[0] for row in coords]),
            "geometry": pa.array(
                [_point_wkb(row[1], row[2]) for row in coords],
                type=pa.binary(),
            ),
        }
    ).replace_schema_metadata(metadata)


def _group_bbox_area(table: pa.Table, tile_size: int) -> float:
    """Return the sum of point bounding-box areas for consecutive tiles."""
    points = [struct.unpack("<BIdd", value)[2:] for value in table["geometry"].to_pylist()]
    total = 0.0
    for start in range(0, len(points), tile_size):
        xs = [point[0] for point in points[start : start + tile_size]]
        ys = [point[1] for point in points[start : start + tile_size]]
        total += (max(xs) - min(xs)) * (max(ys) - min(ys))
    return total


def test_str_order_packs_square_grid_into_snaking_tiles():
    """STR makes X strips and alternates Y direction between adjacent strips."""
    shuffled = [
        (15, 3, 3),
        (0, 0, 0),
        (10, 2, 2),
        (5, 1, 1),
        (12, 3, 0),
        (3, 0, 3),
        (9, 2, 1),
        (6, 1, 2),
        (13, 3, 1),
        (2, 0, 2),
        (8, 2, 0),
        (7, 1, 3),
        (14, 3, 2),
        (1, 0, 1),
        (11, 2, 3),
        (4, 1, 0),
    ]

    result = str_order_table(_point_table(shuffled), tile_size=4)

    assert result["id"].to_pylist() == [0, 4, 1, 5, 2, 6, 3, 7, 11, 15, 10, 14, 9, 13, 8, 12]
    assert result.schema.metadata == _point_table(shuffled).schema.metadata


def test_str_order_reduces_tile_bbox_area_for_adversarial_input():
    """Consecutive STR tiles are spatially tighter than a deliberately scattered order."""
    scattered = []
    next_id = 0
    for offset in range(8):
        for x in range(8):
            scattered.append((next_id, x, (x + offset) % 8))
            next_id += 1
    table = _point_table(scattered)

    result = str_order_table(table, tile_size=8)

    assert _group_bbox_area(result, 8) < _group_bbox_area(table, 8) * 0.25


def test_str_order_places_empty_and_null_geometries_last():
    table = pa.table(
        {
            "id": ["null", "high", "empty", "low"],
            "geometry": pa.array(
                [
                    None,
                    _point_wkb(10, 10),
                    _empty_collection_wkb(),
                    _point_wkb(0, 0),
                ],
                type=pa.binary(),
            ),
        }
    )

    result = str_order_table(table, tile_size=1)

    assert result["id"].to_pylist() == ["low", "high", "null", "empty"]


def test_str_order_all_empty_returns_input_unchanged():
    table = pa.table(
        {
            "id": [1, 2],
            "geometry": pa.array([None, _empty_collection_wkb()], type=pa.binary()),
        }
    )

    assert str_order_table(table, tile_size=2).equals(table)


def test_str_order_preserves_columns_that_resemble_internal_names():
    table = _point_table([(1, 1, 1), (2, 0, 0)]).append_column(
        "__str_input_order", pa.array(["first", "second"])
    )

    result = str_order_table(table, tile_size=1)

    assert result.column_names == table.column_names
    assert result["__str_input_order"].to_pylist() == ["second", "first"]


@pytest.mark.parametrize("tile_size", [0, -1])
def test_str_order_rejects_non_positive_tile_size(tile_size):
    with pytest.raises(InvalidParameterError):
        str_order_table(_point_table([(1, 0, 0)]), tile_size=tile_size)


def test_str_layout_matches_presentation_formula():
    assert _str_layout(row_count=16, tile_size=4) == (4, 2, 8)
    assert _str_layout(row_count=17, tile_size=4) == (5, 3, 6)


def test_str_order_streams_stdin_to_file(tmp_path, monkeypatch):
    table = _point_table([(index, index % 4, index // 4) for index in range(16)])
    stream = io.BytesIO()
    with ipc.RecordBatchStreamWriter(stream, table.schema) as writer:
        writer.write_table(table)
    stream.seek(0)
    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = stream
    monkeypatch.setattr(sys, "stdin", mock_stdin)
    output = tmp_path / "stdin-str.parquet"

    str_order("-", str(output), row_group_rows=4, geoparquet_version="1.1")

    assert pq.read_table(output).num_rows == 16


def test_str_order_streams_file_to_stdout(tmp_path, monkeypatch):
    table = _point_table([(index, index % 4, index // 4) for index in range(16)])
    source = tmp_path / "source.parquet"
    write_geoparquet_table(table, str(source), geometry_column="geometry")
    output = io.BytesIO()
    mock_stdout = mock.MagicMock()
    mock_stdout.isatty.return_value = False
    mock_stdout.buffer = output
    monkeypatch.setattr(sys, "stdout", mock_stdout)

    str_order(str(source), "-", row_group_rows=4)

    output.seek(0)
    assert ipc.RecordBatchStreamReader(output).read_all().num_rows == 16


def test_str_order_file_adds_bbox_and_handles_byte_sized_groups(tmp_path):
    source = tmp_path / "source.parquet"
    output = tmp_path / "str.parquet"
    write_geoparquet_table(
        _point_table([(index, index % 4, index // 4) for index in range(16)]),
        str(source),
        geometry_column="geometry",
    )

    str_order(
        str(source),
        str(output),
        add_bbox_flag=True,
        row_group_size_mb=1,
        geoparquet_version="1.1",
    )

    result = pq.read_table(output)
    assert result.num_rows == 16
    assert "bbox" in result.column_names


def test_str_order_file_passes_through_all_empty_geometries(tmp_path):
    source = tmp_path / "empty-source.parquet"
    output = tmp_path / "empty-str.parquet"
    table = pa.table(
        {
            "id": [1, 2],
            "geometry": pa.array([None, _empty_collection_wkb()], type=pa.binary()),
        }
    )
    write_geoparquet_table(table, str(source), geometry_column="geometry")

    str_order(str(source), str(output), geoparquet_version="1.1")

    assert pq.read_table(output).num_rows == 2
