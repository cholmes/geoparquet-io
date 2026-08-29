import pytest

from geoparquet_io.core import duckdb_metadata
from geoparquet_io.core.validate import (
    CheckStatus,
    _build_bbox_query,
    _check_native_geo_stats_contains_data,
)


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _RecordingConnection:
    def __init__(self, containment_row=(3, 3)):
        self.queries = []
        self._containment_row = containment_row

    def execute(self, query):
        self.queries.append(query)
        return _Result(self._containment_row)


@pytest.fixture
def declared_stats(monkeypatch):
    """Stand in for the file's native geospatial statistics.

    The check reads them through ``get_aggregated_native_geo_stats`` (pyarrow for
    local files since #721), not through the connection it is handed, so the
    fixture files here need carry none.
    """
    monkeypatch.setattr(
        duckdb_metadata,
        "get_aggregated_native_geo_stats",
        lambda *args, **kwargs: {"bbox": [0, 0, 1, 1], "geometry_types": ["Point"]},
    )


def test_bbox_query_excludes_empty_geometries():
    query = _build_bbox_query("input.parquet", "geometry", "BLOB", (0, 0, 1, 1), "")

    assert 'NOT ST_IsEmpty(ST_GeomFromWKB("geometry"))' in query


@pytest.mark.usefixtures("declared_stats")
def test_native_geo_stats_query_excludes_empty_geometries(tmp_path):
    connection = _RecordingConnection(containment_row=(3, 3))
    parquet_file = tmp_path / "input.parquet"
    parquet_file.touch()

    result = _check_native_geo_stats_contains_data(
        str(parquet_file), "geometry", connection, sample_size=0
    )

    assert result.status is CheckStatus.PASSED
    containment_query = connection.queries[-1]
    assert 'NOT ST_IsEmpty("geometry")' in containment_query


@pytest.mark.usefixtures("declared_stats")
def test_native_geo_stats_all_rows_empty_is_skipped(tmp_path):
    """When every row is NULL or EMPTY there is nothing to vouch for: SKIPPED."""
    connection = _RecordingConnection(containment_row=(0, 0))
    parquet_file = tmp_path / "input.parquet"
    parquet_file.touch()

    result = _check_native_geo_stats_contains_data(
        str(parquet_file), "geometry", connection, sample_size=0
    )

    assert result.status is CheckStatus.SKIPPED
    assert "no non-empty geometries" in result.message
