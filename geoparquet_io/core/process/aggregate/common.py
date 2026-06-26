#!/usr/bin/env python3
"""Shared aggregation spec parsing and SQL builders for `gpio process aggregate`."""

from __future__ import annotations

from dataclasses import dataclass

from geoparquet_io.core.exceptions import InvalidParameterError

VALID_METRIC_FUNCS = {"sum", "avg", "min", "max"}
VALID_OUT_GEOMETRY = {"polygon", "centroid", "both", "none"}


@dataclass(frozen=True)
class MetricSpec:
    """A single numeric rollup: ``func`` over ``column`` -> ``output_name``."""

    func: str
    column: str
    output_name: str


def parse_metrics(metric_str: str | None) -> list[MetricSpec]:
    """Parse a --metric string into MetricSpec entries.

    Accepts comma-separated ``func:column`` pairs. A bare ``column`` with no
    ``func:`` prefix defaults to ``sum`` (a total is the common viz intent).
    """
    if not metric_str:
        return []
    specs: list[MetricSpec] = []
    for raw in metric_str.split(","):
        entry = raw.strip()
        if not entry:
            continue
        if ":" in entry:
            func, _, column = entry.partition(":")
            func = func.strip().lower()
            column = column.strip()
        else:
            func = "sum"
            column = entry
        if func not in VALID_METRIC_FUNCS:
            raise InvalidParameterError(
                "metric",
                f"Unknown metric function '{func}'. "
                f"Valid functions: {', '.join(sorted(VALID_METRIC_FUNCS))}",
            )
        if not column:
            raise InvalidParameterError("metric", f"Metric '{entry}' is missing a column name")
        specs.append(MetricSpec(func=func, column=column, output_name=f"{func}_{column}"))
    return specs


def build_metric_select(metrics: list[MetricSpec]) -> str:
    """Build the comma-joined aggregate expressions for the SELECT (no leading comma)."""
    return ", ".join(f'{m.func.upper()}("{m.column}") AS "{m.output_name}"' for m in metrics)
