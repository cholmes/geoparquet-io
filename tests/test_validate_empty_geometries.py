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
    def __init__(self):
        self.queries = []

    def execute(self, query):
        self.queries.append(query)
        if "parquet_metadata" in query:
            return _Result(({"xmin": 0, "ymin": 0, "xmax": 1, "ymax": 1},))
        return _Result((0, 0))


def test_bbox_query_excludes_empty_geometries():
    query = _build_bbox_query("input.parquet", "geometry", "BLOB", (0, 0, 1, 1), "")

    assert "NOT ST_IsEmpty(ST_GeomFromWKB(geometry))" in query


def test_native_geo_stats_query_excludes_empty_geometries(tmp_path):
    connection = _RecordingConnection()
    parquet_file = tmp_path / "input.parquet"
    parquet_file.touch()

    result = _check_native_geo_stats_contains_data(
        str(parquet_file), "geometry", connection, sample_size=0
    )

    assert result.status is CheckStatus.PASSED
    containment_query = connection.queries[-1]
    assert 'NOT ST_IsEmpty("geometry")' in containment_query
