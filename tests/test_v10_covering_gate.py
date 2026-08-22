"""GeoParquet 1.0 output must not carry the 1.1-only ``covering`` key (gpio #686).

``covering`` is a column-metadata field introduced in GeoParquet 1.1 — it does not
appear anywhere in the v1.0.0 specification. The bbox *column* itself is an ordinary
Parquet column and stays legal at 1.0; only the metadata key is version-gated.

Before the fix, ``convert geoparquet --geoparquet-version 1.0`` wrote a file declaring
version 1.0.0 with a ``covering`` entry, which gpio's own validator rejects, while the
command still exited 0.
"""

import json

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.geo_metadata import create_geo_metadata
from geoparquet_io.core.validate import validate_geoparquet
from geoparquet_io.core.write_strategies.base import build_geo_metadata
from tests.conftest import get_geo_metadata, get_geoparquet_version


@pytest.fixture
def points_input(tmp_path):
    """A small native-geometry parquet input with no bbox column."""
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('POINT (30 10)')),
            (2, ST_GeomFromText('POINT (40 40)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
    """)
    con.close()
    return path


def _covering(parquet_file) -> dict | None:
    geo = get_geo_metadata(str(parquet_file))
    primary = geo["primary_column"]
    return geo["columns"][primary].get("covering")


def test_convert_v10_omits_covering_but_keeps_bbox_column(points_input, tmp_path):
    """1.0 output: no covering key, bbox column still written, validator clean."""
    out = tmp_path / "out_10.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="1.0")

    assert get_geoparquet_version(str(out)) == "1.0.0"
    assert _covering(out) is None, "covering is 1.1-only and must not appear in 1.0 output"

    # The bbox column itself stays — it is a plain column, legal at any version.
    assert "bbox" in pq.ParquetFile(str(out)).schema_arrow.names

    result = validate_geoparquet(str(out))
    failures = [c.message for c in result.checks if c.status.value == "failed"]
    assert result.is_valid, f"1.0 output failed validation: {failures}"


def test_check_bbox_does_not_demand_covering_on_v10(points_input, tmp_path):
    """`check bbox` must not ask a 1.0 file for a key that version cannot carry."""
    from geoparquet_io.core.check_parquet_structure import check_metadata_and_bbox

    out = tmp_path / "check_10.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="1.0")

    results = check_metadata_and_bbox(str(out), return_results=True, quiet=True)
    assert results["has_bbox_column"]
    assert not results["needs_bbox_metadata"]
    assert not any("covering" in issue for issue in results["issues"])
    # The actionable advice for 1.0 stays the version upgrade, which is already reported.
    assert any("outdated" in issue for issue in results["issues"])


def test_convert_v11_still_writes_covering(points_input, tmp_path):
    """1.1 output keeps the covering key pointing at the bbox column."""
    out = tmp_path / "out_11.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="1.1")

    assert get_geoparquet_version(str(out)) == "1.1.0"
    covering = _covering(out)
    assert covering is not None and covering["bbox"]["xmin"] == ["bbox", "xmin"]
    assert validate_geoparquet(str(out)).is_valid


def test_convert_v20_output_valid(points_input, tmp_path):
    """2.0 output stays valid (native geo types, no bbox column)."""
    out = tmp_path / "out_20.parquet"
    convert_to_geoparquet(str(points_input), str(out), skip_hilbert=True, geoparquet_version="2.0")
    assert get_geoparquet_version(str(out)) == "2.0.0"
    assert validate_geoparquet(str(out)).is_valid


def test_cli_convert_v10_exits_zero_and_passes_check_spec(points_input, tmp_path):
    """CLI parity for the issue's repro: exit 0 and a file its own checker accepts."""
    out = tmp_path / "cli_10.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "convert",
            "geoparquet",
            str(points_input),
            str(out),
            "--geoparquet-version",
            "1.0",
            "--skip-hilbert",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "did not pass GeoParquet metadata validation" not in result.output
    assert _covering(out) is None

    check = runner.invoke(cli, ["check", "spec", str(out)])
    assert check.exit_code == 0, check.output
    assert "covering" not in check.output


def test_v10_downgrade_drops_covering_carried_from_source(points_input, tmp_path):
    """A 1.1 file with covering, re-converted to 1.0, must lose the covering key."""
    v11 = tmp_path / "v11.parquet"
    convert_to_geoparquet(str(points_input), str(v11), skip_hilbert=True, geoparquet_version="1.1")
    assert _covering(v11) is not None

    out = tmp_path / "downgraded.parquet"
    convert_to_geoparquet(str(v11), str(out), skip_hilbert=True, geoparquet_version="1.0")
    assert get_geoparquet_version(str(out)) == "1.0.0"
    assert _covering(out) is None
    assert validate_geoparquet(str(out)).is_valid


def test_build_geo_metadata_gates_covering_by_version():
    """The shared assembly point drops covering for 1.0 and keeps it for 1.1/2.0."""
    source = {
        "geo": json.dumps(
            {
                "version": "1.1.0",
                "primary_column": "geometry",
                "columns": {
                    "geometry": {
                        "encoding": "WKB",
                        "covering": {
                            "bbox": {
                                "xmin": ["bbox", "xmin"],
                                "ymin": ["bbox", "ymin"],
                                "xmax": ["bbox", "xmax"],
                                "ymax": ["bbox", "ymax"],
                            }
                        },
                    }
                },
            }
        )
    }

    v10 = build_geo_metadata("geometry", "1.0", original_metadata=source)
    assert "covering" not in v10["columns"]["geometry"]

    for version in ("1.1", "2.0"):
        kept = build_geo_metadata("geometry", version, original_metadata=source)
        assert "covering" in kept["columns"]["geometry"], version


def test_create_geo_metadata_gates_covering_by_version():
    """The Arrow-table assembly point applies the same gate."""
    bbox_info = {"has_bbox_column": True, "bbox_column_name": "bbox"}

    v10 = create_geo_metadata(None, "geometry", bbox_info, version="1.0.0")
    assert "covering" not in v10["columns"]["geometry"]

    v11 = create_geo_metadata(None, "geometry", bbox_info, version="1.1.0")
    assert v11["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    # Verbose mode reports the drop rather than silently changing the output.
    verbose_v10 = create_geo_metadata(None, "geometry", bbox_info, None, True, version="1.0.0")
    assert "covering" not in verbose_v10["columns"]["geometry"]


def test_create_geo_metadata_drops_custom_covering_for_v10():
    """Spatial-index coverings (h3/s2/...) are also 1.1-only."""
    custom = {"covering": {"h3": {"column": "h3", "resolution": 8}}}

    v10 = create_geo_metadata(None, "geometry", None, custom, version="1.0.0")
    assert "covering" not in v10["columns"]["geometry"]

    v11 = create_geo_metadata(None, "geometry", None, custom, version="1.1.0")
    assert "h3" in v11["columns"]["geometry"]["covering"]
