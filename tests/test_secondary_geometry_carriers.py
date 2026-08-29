"""Every write strategy must give a SECONDARY geometry column the right carrier.

Regression tests for #706. A file may declare more than one geometry column
(`geo["columns"]` with a `primary_column` plus secondaries; gpio carries them
through `geometry_info["secondary"]`). Validation applies the same per-version
requirements to *every* column in `geo["columns"]`, but each strategy decided the
physical carrier for the **primary only** and let a secondary keep whatever
DuckDB or the input happened to hand over.

The measured result before this suite existed:

| strategy | secondary at 1.1 | import-state dependent? |
|---|---|---|
| duckdb-kv | native GEOMETRY inside a 1.x file -> invalid | no |
| in-memory | flips plain binary <-> native | **yes** |
| disk-rewrite | plain binary -> valid | no |
| streaming | plain binary -> valid (fixed by #688/#707) | no |

Nothing in the suite covered secondary-column carriers outside the streaming
strategy, which is why the other three went unnoticed.
"""

import hashlib
import json
import struct
import subprocess
import sys
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.write_strategies import WriteStrategy, WriteStrategyFactory
from geoparquet_io.core.write_strategies.base import resolve_geometry_columns

STRATEGIES = ["duckdb-kv", "in-memory", "streaming", "disk-rewrite"]

# disk-rewrite never emits a native Parquet GEOMETRY type at 2.0 -- for the
# PRIMARY column as well, on an ordinary single-geometry file. That is a
# strategy-wide gap, not a secondary-column one, so it is tracked in #764 and
# excluded here rather than silently expected.
NATIVE_CAPABLE_STRATEGIES = ["duckdb-kv", "in-memory", "streaming"]


def _wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BI2d", 1, 1, x, y)


def _geo_dict() -> dict:
    return {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {"encoding": "WKB", "geometry_types": ["Point"]},
            "geom2": {"encoding": "WKB", "geometry_types": ["Point"]},
        },
    }


def _geometry_info() -> dict:
    geo = _geo_dict()
    return {
        "primary": "geometry",
        "secondary": ["geom2"],
        "metadata": {"geom2": geo["columns"]["geom2"]},
    }


@pytest.fixture
def two_geometry_source(tmp_path):
    """A file declaring two WKB geometry columns."""
    path = tmp_path / "two_geom.parquet"
    table = pa.table(
        {
            "id": [1, 2],
            "geometry": [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)],
            "geom2": [_wkb_point(5.0, 6.0), _wkb_point(7.0, 8.0)],
        }
    )
    pq.write_table(table.replace_schema_metadata({b"geo": json.dumps(_geo_dict()).encode()}), path)
    return str(path)


def _carriers(path: str) -> dict[str, str]:
    """Physical carrier per geometry column: "native" or the Parquet physical type.

    Read without the spatial extension so the answer is the file's own schema,
    not something DuckDB reconstructed.
    """
    con = get_duckdb_connection(load_spatial=False)
    try:
        rows = con.execute(
            f"SELECT name, type, logical_type FROM parquet_schema('{path}')"
        ).fetchall()
    finally:
        con.close()
    return {
        name: "native" if (logical and "Geometry" in str(logical)) else str(typ).lower()
        for name, typ, logical in rows
        if name in ("geometry", "geom2")
    }


def _failed_checks(path: str) -> list[str]:
    return sorted({c.name for c in validate_geoparquet(path).checks if c.status.value == "failed"})


def _write_via_query(source: str, out: str, version: str, strategy: str) -> None:
    con = get_duckdb_connection(load_spatial=True)
    try:
        metadata, _ = get_parquet_metadata(source)
        write_parquet_with_metadata(
            con,
            f"SELECT * FROM read_parquet('{source}')",
            out,
            original_metadata=metadata,
            geoparquet_version=version,
            write_strategy=strategy,
            input_file=source,
            geometry_info=_geometry_info(),
        )
    finally:
        con.close()


class TestSecondaryCarrierMatchesTheTargetVersion:
    """The carrier decision is per version and applies to every geometry column."""

    @pytest.mark.parametrize("strategy", STRATEGIES)
    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_v1x_writes_plain_wkb_for_both_columns(
        self, strategy, version, two_geometry_source, tmp_path
    ):
        """1.0/1.1 require plain BYTE_ARRAY WKB for every column in geo["columns"]."""
        out = str(tmp_path / f"{strategy}_{version}.parquet")

        _write_via_query(two_geometry_source, out, version, strategy)

        carriers = _carriers(out)
        assert carriers["geometry"] == "byte_array"
        assert carriers["geom2"] == "byte_array", (
            f"{strategy} gave the secondary column a {carriers['geom2']} carrier in a "
            f"{version} file"
        )

    @pytest.mark.parametrize("strategy", STRATEGIES)
    @pytest.mark.parametrize("version", ["1.0", "1.1"])
    def test_v1x_output_is_valid(self, strategy, version, two_geometry_source, tmp_path):
        """The file a native secondary produced failed version_features_match."""
        out = str(tmp_path / f"valid_{strategy}_{version}.parquet")

        _write_via_query(two_geometry_source, out, version, strategy)

        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", NATIVE_CAPABLE_STRATEGIES)
    def test_v20_writes_native_for_both_columns(self, strategy, two_geometry_source, tmp_path):
        """2.0 requires a native Parquet GEOMETRY type for every declared column."""
        out = str(tmp_path / f"{strategy}_20.parquet")

        _write_via_query(two_geometry_source, out, "2.0", strategy)

        carriers = _carriers(out)
        assert carriers["geometry"] == "native"
        assert carriers["geom2"] == "native", (
            f"{strategy} left the secondary column as {carriers['geom2']} in a 2.0 file"
        )
        assert _failed_checks(out) == []

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_rows_survive(self, strategy, two_geometry_source, tmp_path):
        """Converting a second column must not drop or reorder rows."""
        out = str(tmp_path / f"rows_{strategy}.parquet")

        _write_via_query(two_geometry_source, out, "1.1", strategy)

        table = pq.read_table(out)
        assert table.num_rows == 2
        assert table.column("id").to_pylist() == [1, 2]


class TestSecondaryCarrierOnTheTableEntryPoint:
    """``write_from_table`` gets no ``geometry_info`` — the names come from the table."""

    @staticmethod
    def _table_with_extension_secondary():
        """A table whose SECONDARY column already carries the geoarrow extension type.

        This is the shape a caller produces in a process that imported
        ``geoarrow.pyarrow``; PyArrow writes such a column as a native Parquet
        GEOMETRY logical type, which is illegal below 2.0.
        """
        import geoarrow.pyarrow as ga

        table = pa.table(
            {
                "id": [1, 2],
                "geometry": pa.array(
                    [_wkb_point(1.0, 2.0), _wkb_point(3.0, 4.0)], type=pa.binary()
                ),
                "geom2": ga.as_wkb(
                    pa.array([_wkb_point(5.0, 6.0), _wkb_point(7.0, 8.0)], type=pa.binary())
                ),
            }
        )
        return table.replace_schema_metadata({b"geo": json.dumps(_geo_dict()).encode()})

    @pytest.mark.parametrize("strategy", STRATEGIES)
    def test_extension_typed_secondary_is_written_as_plain_wkb_at_1_1(self, strategy, tmp_path):
        out = str(tmp_path / f"table_{strategy}.parquet")
        table = self._table_with_extension_secondary()
        assert table.schema.field("geom2").type.extension_name == "geoarrow.wkb"

        WriteStrategyFactory.get_strategy(WriteStrategy(strategy)).write_from_table(
            table=table,
            output_path=out,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert _carriers(out)["geom2"] == "byte_array", (
            f"{strategy} carried the geoarrow extension type into a 1.1 file"
        )
        assert _failed_checks(out) == []


_DETERMINISM_SCRIPT = textwrap.dedent(
    """
    import json, struct, sys
    source, out, do_import = sys.argv[1], sys.argv[2], sys.argv[3] == "import"
    if do_import:
        import geoarrow.pyarrow  # noqa: F401
    from geoparquet_io.core.common import get_parquet_metadata, write_parquet_with_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    metadata, _ = get_parquet_metadata(source)
    write_parquet_with_metadata(
        con,
        "SELECT * FROM read_parquet('" + source + "')",
        out,
        original_metadata=metadata,
        geoparquet_version=sys.argv[4],
        write_strategy=sys.argv[5],
        input_file=source,
        geometry_info={
            "primary": "geometry",
            "secondary": ["geom2"],
            "metadata": {"geom2": {"encoding": "WKB", "geometry_types": ["Point"]}},
        },
    )
    con.close()
    """
)


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_secondary_carrier_is_independent_of_geoarrow_import(
    strategy, version, two_geometry_source, tmp_path
):
    """Same input, same bytes out, whatever the process imported.

    ``in-memory`` flipped the secondary column between plain binary and a native
    GEOMETRY type depending on whether anything had imported
    ``geoarrow.pyarrow`` — the #688 nondeterminism, on the secondary column.

    Runs in subprocesses because geoarrow's extension registration is
    process-global and cannot be undone once done.
    """
    if strategy == "disk-rewrite" and version == "2.0":
        pytest.skip("disk-rewrite never emits native types at 2.0 (#764)")

    outputs = {}
    for label in ("clean", "import"):
        out = tmp_path / f"{strategy}_{version}_{label}.parquet"
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                _DETERMINISM_SCRIPT,
                two_geometry_source,
                str(out),
                label,
                version,
                strategy,
            ],
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=300,
        )
        assert result.returncode == 0, f"subprocess failed:\n{result.stderr}"
        outputs[label] = out

    clean, imported = outputs["clean"], outputs["import"]
    assert (
        hashlib.sha256(clean.read_bytes()).hexdigest()
        == hashlib.sha256(imported.read_bytes()).hexdigest()
    ), (
        f"{strategy} at {version} is import-state dependent: clean wrote "
        f"{_carriers(str(clean))}, after importing geoarrow.pyarrow it wrote "
        f"{_carriers(str(imported))}"
    )
    assert _failed_checks(str(clean)) == []


class TestResolveGeometryColumns:
    """The one place every strategy asks "which columns are geometry?"."""

    def test_primary_only(self):
        assert resolve_geometry_columns("geometry") == {"geometry"}

    def test_includes_secondaries_from_geometry_info(self):
        info = {"primary": "geometry", "secondary": ["geom2", "geom3"], "metadata": {}}
        assert resolve_geometry_columns("geometry", info) == {"geometry", "geom2", "geom3"}

    def test_includes_columns_named_only_in_geo_metadata(self):
        assert resolve_geometry_columns("geometry", None, _geo_dict()) == {"geometry", "geom2"}

    def test_geometry_info_wins_when_geo_metadata_is_absent(self):
        """parquet-geo-only writes no geo metadata, so the names must come from geometry_info."""
        info = {"primary": "geometry", "secondary": ["geom2"], "metadata": {}}
        assert resolve_geometry_columns("geometry", info, None) == {"geometry", "geom2"}

    def test_tolerates_empty_inputs(self):
        assert resolve_geometry_columns("geometry", {}, {}) == {"geometry"}
        assert resolve_geometry_columns("geometry", {"secondary": None}, None) == {"geometry"}
