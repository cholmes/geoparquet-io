"""``gpio sort`` must default to the row-group size it advertises (#775).

Before this suite, a bare ``gpio sort hilbert`` passed ``ROW_GROUP_SIZE`` to
DuckDB as *nothing at all*, so DuckDB's own 122,880-row default applied while
``--help`` advertised 100,000 and the guide recommended 10,000-50,000 rows per
group for the spatial queries sorting exists to serve. Four numbers, none of
them agreeing.

The sort commands now resolve their own default -- ``DEFAULT_SORT_ROW_GROUP_ROWS``,
the top of gpio's recommended band -- and hand it down explicitly, so the
advertised default *is* the effective default on every write path.

Note the writer rounds a row-group target up to a multiple of 2048, so a 50,000
target lands on 51,200-row groups. The assertions below use that rounding
rather than an exact equality.
"""

from __future__ import annotations

import io
import json
import math
import random
import re
import struct
import sys
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.hilbert_order import hilbert_order
from geoparquet_io.core.parquet_writer import DEFAULT_SORT_ROW_GROUP_ROWS
from geoparquet_io.core.str_order import DEFAULT_STR_TILE_SIZE

# Enough rows that the old 122,880-row default and the new 50,000-row one are
# unambiguously different layouts (3 groups vs 5), while staying fast to build
# and sort.
ROW_COUNT = 250_000

# DuckDB's Parquet writer rounds a ROW_GROUP_SIZE up to a multiple of its
# vector-chunk size, so an exact row count is not what lands on disk.
WRITER_CHUNK_ROWS = 2048


def _round_up_to_chunk(rows: int) -> int:
    return math.ceil(rows / WRITER_CHUNK_ROWS) * WRITER_CHUNK_ROWS


def _pseudo_quadkey(value: int, digits: int = 13) -> str:
    """A quadkey-shaped string, so ``sort quadkey`` need not compute a real one.

    ``gpio sort quadkey`` auto-adds the column when it is missing, which costs
    far more than the write this suite is measuring. The column only has to
    exist and sort; its cells are never interpreted here.
    """
    out = []
    for _ in range(digits):
        out.append(str(value % 4))
        value //= 4
    return "".join(reversed(out))


def _write_points(path, n=ROW_COUNT, seed=775):
    """Write a small GeoParquet file of random WKB points as one row group."""
    rng = random.Random(seed)
    geometry = [
        struct.pack("<BIdd", 1, 1, rng.uniform(-180, 180), rng.uniform(-85, 85)) for _ in range(n)
    ]
    table = pa.table(
        {
            "id": pa.array(range(n), pa.int64()),
            "name": pa.array([f"f{i % 997}" for i in range(n)], pa.string()),
            "quadkey": pa.array([_pseudo_quadkey(i) for i in range(n)], pa.string()),
            "geometry": pa.array(geometry, pa.binary()),
        }
    )
    metadata = {
        b"geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
            }
        ).encode("utf-8")
    }
    pq.write_table(table.replace_schema_metadata(metadata), str(path))
    return str(path)


def _row_group_rows(path) -> list[int]:
    parquet_file = pq.ParquetFile(str(path))
    return [parquet_file.metadata.row_group(i).num_rows for i in range(parquet_file.num_row_groups)]


@pytest.fixture(scope="module")
def points_file(tmp_path_factory):
    return _write_points(tmp_path_factory.mktemp("sort_rg") / "points.parquet")


# Every ``gpio sort`` subcommand, with the extra arguments it needs beyond
# input and output.
SORT_COMMANDS = {
    "hilbert": [],
    "str": [],
    "quadkey": [],
    "column": ["id"],
}


def _help_default_rows(command: str) -> int:
    """The row count ``--row-group-size --help`` advertises as its default."""
    result = CliRunner().invoke(cli, ["sort", command, "--help"])
    assert result.exit_code == 0, result.output
    # Collapse the wrapped help block so the option text is one line.
    flat = " ".join(result.output.split())
    match = re.search(r"--row-group-size INTEGER\s+(.*?)--row-group-size-mb", flat)
    assert match, f"could not locate --row-group-size help in:\n{result.output}"
    number = re.search(r"default: ([\d,]+)", match.group(1))
    assert number, f"--row-group-size help states no default: {match.group(1)!r}"
    return int(number.group(1).replace(",", ""))


@pytest.mark.parametrize("command", sorted(SORT_COMMANDS))
def test_help_advertises_the_sort_default(command):
    """Every sort subcommand's help names the default the sort commands use."""
    assert _help_default_rows(command) == DEFAULT_SORT_ROW_GROUP_ROWS


@pytest.mark.parametrize("command", sorted(SORT_COMMANDS))
def test_default_row_groups_match_the_advertised_default(command, points_file, tmp_path):
    """A bare ``gpio sort <cmd>`` writes groups of the size ``--help`` promises.

    This is the #775 regression: the advertised default was inert, so DuckDB's
    122,880-row default applied instead.
    """
    output = tmp_path / f"{command}.parquet"
    result = CliRunner().invoke(
        cli, ["sort", command, points_file, str(output), *SORT_COMMANDS[command]]
    )
    assert result.exit_code == 0, result.output

    advertised = _help_default_rows(command)
    ceiling = _round_up_to_chunk(advertised)
    groups = _row_group_rows(output)

    assert sum(groups) == ROW_COUNT
    # Every full group is the advertised size (rounded up to a writer chunk);
    # only the trailing remainder group may be smaller.
    assert max(groups) <= ceiling, f"{command}: groups {groups} exceed {ceiling}"
    assert max(groups) > advertised * 0.9, f"{command}: groups {groups} far below {advertised}"
    assert len(groups) == math.ceil(ROW_COUNT / ceiling), f"{command}: groups {groups}"


def test_default_is_inside_the_recommended_spatial_band(points_file, tmp_path):
    """The sort default must sit inside the 10,000-50,000 band gpio recommends.

    ``gpio check`` prints that band as advice; a default outside it means the
    tool contradicts itself the moment a user runs ``sort`` then ``check``.
    """
    assert 10_000 <= DEFAULT_SORT_ROW_GROUP_ROWS <= 50_000

    output = tmp_path / "band.parquet"
    result = CliRunner().invoke(cli, ["sort", "hilbert", points_file, str(output)])
    assert result.exit_code == 0, result.output
    assert max(_row_group_rows(output)) <= _round_up_to_chunk(50_000)


def test_explicit_row_group_size_still_wins(points_file, tmp_path):
    """The new default must not shadow an explicit ``--row-group-size``."""
    output = tmp_path / "explicit.parquet"
    result = CliRunner().invoke(
        cli,
        ["sort", "hilbert", points_file, str(output), "--row-group-size", "20000"],
    )
    assert result.exit_code == 0, result.output
    assert max(_row_group_rows(output)) <= _round_up_to_chunk(20_000)


def test_row_group_size_mb_is_not_overridden_by_the_default(points_file, tmp_path):
    """``--row-group-size-mb`` must still size groups, not collide with the default.

    The default is resolved only when *neither* sizing option is given, so an
    MB target must not raise the mutually-exclusive usage error nor be silently
    replaced by a row count.
    """
    output = tmp_path / "mb.parquet"
    result = CliRunner().invoke(
        cli,
        ["sort", "hilbert", points_file, str(output), "--row-group-size-mb", "1MB"],
    )
    assert result.exit_code == 0, result.output
    # A 1MB target on this data is far smaller than 50,000 rows, so the MB path
    # is demonstrably the one that sized the groups.
    assert max(_row_group_rows(output)) < DEFAULT_SORT_ROW_GROUP_ROWS


def test_streaming_path_receives_the_resolved_default(points_file, tmp_path, monkeypatch):
    """The streaming branch gets the resolved default, not a bare ``None``.

    ``hilbert_order`` forks into a streaming path whenever input is stdin or
    output is stdout, and that path writes through a different code path than
    the file-based one. The default is resolved *before* the fork precisely so
    both sides get it; a resolver placed in the CLI, or after the branch, would
    leave ``gpio sort hilbert - out.parquet`` on DuckDB's 122,880-row default.
    """
    with pq.ParquetFile(points_file) as source:
        table = source.read()

    ipc_buffer = io.BytesIO()
    writer = ipc.RecordBatchStreamWriter(ipc_buffer, table.schema)
    writer.write_table(table)
    writer.close()
    ipc_buffer.seek(0)

    mock_stdin = mock.MagicMock()
    mock_stdin.isatty.return_value = False
    mock_stdin.buffer = ipc_buffer
    monkeypatch.setattr(sys, "stdin", mock_stdin)

    output = tmp_path / "streamed.parquet"
    hilbert_order("-", str(output))

    rows = _row_group_rows(output)
    assert sum(rows) == table.num_rows
    assert max(rows) == _round_up_to_chunk(DEFAULT_SORT_ROW_GROUP_ROWS)


def test_str_tile_size_tracks_the_sort_default():
    """``sort str``'s in-memory tile size is the sort default, not a stray constant.

    ``DEFAULT_STR_TILE_SIZE`` is the public default of ``ops.sort_str()`` and
    ``Table.sort_str()``, and it also picks the strip count the CLI builds. If
    it drifts from ``DEFAULT_SORT_ROW_GROUP_ROWS``, ``gpio sort str`` lays out
    different strips depending on whether ``--row-group-size-mb`` was passed,
    and the documented Python default stops matching the CLI's.
    """
    assert DEFAULT_STR_TILE_SIZE == DEFAULT_SORT_ROW_GROUP_ROWS == 50_000
