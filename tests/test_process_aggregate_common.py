import pytest

from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.common import (
    MetricSpec,
    build_metric_select,
    parse_metrics,
)


def test_parse_metrics_empty():
    assert parse_metrics(None) == []
    assert parse_metrics("") == []


def test_parse_metrics_func_and_bare():
    specs = parse_metrics("sum:area_ha, avg:yield, population")
    assert specs == [
        MetricSpec("sum", "area_ha", "sum_area_ha"),
        MetricSpec("avg", "yield", "avg_yield"),
        MetricSpec("sum", "population", "sum_population"),  # bare -> sum
    ]


def test_parse_metrics_rejects_unknown_func():
    with pytest.raises(InvalidParameterError):
        parse_metrics("median:area_ha")


def test_parse_metrics_rejects_missing_column():
    with pytest.raises(InvalidParameterError):
        parse_metrics("sum:")


def test_build_metric_select():
    specs = parse_metrics("sum:area_ha, avg:yield")
    sql = build_metric_select(specs)
    assert sql == 'SUM("area_ha") AS "sum_area_ha", AVG("yield") AS "avg_yield"'
    assert build_metric_select([]) == ""
