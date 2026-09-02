"""convert must never copy an invalid PROJJSON CRS into its output (#705).

`convert` reads the input's ``crs`` and writes it to the output verbatim. When
the input CRS is not valid PROJJSON, the output inherits the defect: gpio's own
post-write check warns, but the command still exits 0, so `gpio convert && next`
proceeds on a file `gpio check spec` rejects.

The contract these tests pin is *not* the exit code of the post-write warning
(deliberately unchanged): it is that convert either produces a file that passes
`validate_geoparquet`, or fails loudly naming the bad input. It must never
silently write an invalid file.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.validate import validate_geoparquet

CORPUS = Path(__file__).parent / "data" / "geoparquet-testing"

# The corpus fixture's CRS: no PROJJSON "type", and an authority nothing can
# resolve. Nothing about it says what CRS was meant, so it is unrepairable.
UNRESOLVABLE_CRS = {"id": {"authority": "XYZ", "code": 999}, "name": "Not a real CRS"}

# Missing only the "type" member, but carrying an id that unambiguously names a
# real CRS: repairable by resolving the authority code.
REPAIRABLE_CRS = {"id": {"authority": "EPSG", "code": 3857}, "name": "WGS 84 / Pseudo-Mercator"}


def _write_geoparquet_with_crs(path: Path, crs: dict) -> Path:
    """Write a small valid GeoParquet file, then force ``crs`` into its metadata."""
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES (1, ST_GeomFromText('POINT (1 2)'))) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
    """)
    con.close()

    table = pq.read_table(str(path))
    meta = dict(table.schema.metadata)
    geo = json.loads(meta[b"geo"])
    for col in geo["columns"].values():
        col["crs"] = crs
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), str(path))
    return path


def _assert_no_silently_invalid_output(src: Path, out: Path) -> None:
    """convert either fails loudly, or leaves an output that validates."""
    try:
        convert_to_geoparquet(str(src), str(out), skip_hilbert=True)
    except GeoParquetError as exc:
        # Failing is allowed — but the message has to name the offending input
        # and say what is wrong, and no half-written output may be left behind.
        assert src.name in str(exc), exc
        assert "CRS" in str(exc), exc
        return

    result = validate_geoparquet(str(out), validate_data=False)
    assert result.is_valid, [c.message for c in result.checks if c.status.name == "FAILED"]


@pytest.mark.corpus
@pytest.mark.integration
def test_corpus_invalid_projjson_crs_does_not_propagate(tmp_path):
    """The geoparquet-testing bad_data fixture must not yield an invalid output."""
    src = CORPUS / "bad_data" / "crs-invalid-projjson.parquet"
    if not src.exists():
        pytest.skip("run: git submodule update --init")
    _assert_no_silently_invalid_output(src, tmp_path / "out.parquet")


def test_unresolvable_projjson_crs_does_not_propagate(tmp_path):
    """Same invariant without the submodule: an equivalent hand-built input."""
    src = _write_geoparquet_with_crs(tmp_path / "src.parquet", UNRESOLVABLE_CRS)
    _assert_no_silently_invalid_output(src, tmp_path / "out.parquet")


def test_unresolvable_projjson_crs_error_is_actionable(tmp_path):
    """An unrepairable CRS fails with a message a user can act on."""
    src = _write_geoparquet_with_crs(tmp_path / "src.parquet", UNRESOLVABLE_CRS)
    with pytest.raises(GeoParquetError) as excinfo:
        convert_to_geoparquet(str(src), str(tmp_path / "out.parquet"), skip_hilbert=True)
    message = str(excinfo.value)
    assert "src.parquet" in message
    assert "PROJJSON" in message
    assert "XYZ" in message  # names the CRS it could not make sense of


def test_repairable_projjson_crs_is_repaired(tmp_path):
    """A CRS missing only "type", with a resolvable id, is repaired not rejected."""
    src = _write_geoparquet_with_crs(tmp_path / "src.parquet", REPAIRABLE_CRS)
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True)

    result = validate_geoparquet(str(out), validate_data=False)
    assert result.is_valid, [c.message for c in result.checks if c.status.name == "FAILED"]

    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    crs = geo["columns"][geo["primary_column"]]["crs"]
    assert crs["type"] == "ProjectedCRS", crs
    assert str(crs["id"]["code"]) == "3857", crs


def test_valid_projjson_crs_is_untouched(tmp_path):
    """The repair path must not rewrite a CRS that was already valid."""
    import pyproj

    crs = pyproj.CRS.from_authority("EPSG", "3857").to_json_dict()
    src = _write_geoparquet_with_crs(tmp_path / "src.parquet", crs)
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True)

    result = validate_geoparquet(str(out), validate_data=False)
    assert result.is_valid, [c.message for c in result.checks if c.status.name == "FAILED"]
    geo = json.loads(pq.read_schema(str(out)).metadata[b"geo"])
    assert geo["columns"][geo["primary_column"]]["crs"]["type"] == "ProjectedCRS"
