"""Validate GeoParquet files against the fiboa specification."""

from __future__ import annotations

import json
import re

import click
import pyarrow.parquet as pq

from geoparquet_io.core.constants import FIBOA_CORE_SCHEMA

REQUIRED_COLUMNS = {"geometry"}
EXPECTED_COLUMNS = {
    "id": "string",
    "collection": "string",
    "geometry": "binary",
    "admin:country_code": "string",
    "admin:subdivision_code": "string",
    "metrics:area": "float",
    "metrics:perimeter": "float",
    "determination:datetime": "timestamp",
    "determination:method": "string",
}

COUNTRY_CODE_PATTERN = re.compile(r"^[A-Z]{2}$")
SUBDIVISION_CODE_PATTERN = re.compile(r"^[A-Z0-9]{1,3}$")

VALID_DETERMINATION_METHODS = {
    "manual",
    "surveyed",
    "driven",
    "auto-operation",
    "auto-imagery",
    "unknown",
}


def validate_fiboa(input_file: str, verbose: bool = False) -> bool:
    """Validate a GeoParquet file against fiboa specification.

    Returns True if valid, False otherwise.
    """
    issues: list[str] = []
    warnings: list[str] = []
    info_msgs: list[str] = []

    try:
        pf = pq.ParquetFile(input_file)
    except Exception as e:
        click.echo(click.style(f"Cannot read file: {e}", fg="red"), err=True)
        return False

    schema = pf.schema_arrow
    col_names = set(schema.names)
    metadata = schema.metadata or {}

    if "geometry" not in col_names:
        issues.append("Missing required column: geometry")

    present_cols = []
    missing_cols = []
    for col in EXPECTED_COLUMNS:
        if col in col_names:
            present_cols.append(col)
        elif col in REQUIRED_COLUMNS:
            issues.append(f"Missing required column: {col}")
        else:
            missing_cols.append(col)

    if missing_cols and verbose:
        info_msgs.append(f"Optional columns not present: {', '.join(missing_cols)}")

    collection_meta = metadata.get(b"collection")
    if collection_meta:
        try:
            vecorel = json.loads(collection_meta)
            schema_urls = vecorel.get("schemas", {}).get("default", [])
            if FIBOA_CORE_SCHEMA not in schema_urls:
                warnings.append(
                    f"Collection schemas metadata missing fiboa URL: {FIBOA_CORE_SCHEMA}"
                )
            if verbose:
                info_msgs.append(f"Collection schemas: {schema_urls}")
        except json.JSONDecodeError:
            issues.append("Collection metadata is not valid JSON")
    else:
        warnings.append("No collection metadata found (missing 'collection' key in file metadata)")

    try:
        table = pf.read_row_group(0)
        _validate_data_values(table, col_names, issues, warnings, verbose)
    except Exception as e:
        if verbose:
            warnings.append(f"Could not validate data values: {e}")

    _print_result(input_file, issues, warnings, info_msgs)
    return len(issues) == 0


def _validate_data_values(
    table, col_names: set, issues: list, warnings: list, verbose: bool
) -> None:
    """Validate data values in a table sample."""
    import pyarrow as pa
    import pyarrow.compute as pc

    if "admin:country_code" in col_names:
        col = table.column("admin:country_code")
        valid_mask = pc.is_valid(col)
        valid_vals = pc.filter(col, valid_mask)
        if len(valid_vals) > 0:
            matches = pc.match_substring_regex(pc.cast(valid_vals, pa.string()), r"^[A-Z]{2}$")
            invalid_count = pc.sum(pc.invert(matches)).as_py()
            if invalid_count > 0:
                issues.append(
                    f"admin:country_code has {invalid_count} values not matching "
                    f"ISO 3166-1 alpha-2 pattern (^[A-Z]{{2}}$)"
                )

    if "admin:subdivision_code" in col_names:
        col = table.column("admin:subdivision_code")
        valid_mask = pc.is_valid(col)
        valid_vals = pc.filter(col, valid_mask)
        if len(valid_vals) > 0:
            matches = pc.match_substring_regex(pc.cast(valid_vals, pa.string()), r"^[A-Z0-9]{1,3}$")
            invalid_count = pc.sum(pc.invert(matches)).as_py()
            if invalid_count > 0:
                warnings.append(
                    f"admin:subdivision_code has {invalid_count} values not matching "
                    f"ISO 3166-2 pattern (^[A-Z0-9]{{1,3}}$)"
                )

    for metric_col in ["metrics:area", "metrics:perimeter"]:
        if metric_col in col_names:
            col = table.column(metric_col)
            non_null = pc.filter(col, pc.is_valid(col))
            if len(non_null) > 0:
                min_val = pc.min(non_null).as_py()
                if min_val is not None and min_val <= 0:
                    issues.append(f"{metric_col} has values <= 0 (min: {min_val})")

    if "determination:method" in col_names:
        col = table.column("determination:method")
        valid_mask = pc.is_valid(col)
        valid_vals = pc.filter(col, valid_mask)
        if len(valid_vals) > 0:
            valid_set = pa.array(list(VALID_DETERMINATION_METHODS))
            in_valid = pc.is_in(pc.cast(valid_vals, pa.string()), value_set=valid_set)
            invalid_vals = pc.filter(valid_vals, pc.invert(in_valid))
            invalid_values = {v.as_py() for v in invalid_vals}
            if invalid_values:
                warnings.append(f"determination:method has non-standard values: {invalid_values}")


def _print_result(
    input_file: str,
    issues: list[str],
    warnings: list[str],
    info_msgs: list[str],
) -> None:
    """Print validation results."""
    click.echo(f"\nfiboa validation: {input_file}")
    click.echo("=" * 60)

    if issues:
        click.echo(click.style(f"\nErrors ({len(issues)}):", fg="red"))
        for issue in issues:
            click.echo(click.style(f"  x {issue}", fg="red"))

    if warnings:
        click.echo(click.style(f"\nWarnings ({len(warnings)}):", fg="yellow"))
        for warning in warnings:
            click.echo(click.style(f"  ! {warning}", fg="yellow"))

    if info_msgs:
        click.echo("\nInfo:")
        for msg in info_msgs:
            click.echo(f"  - {msg}")

    if not issues and not warnings:
        click.echo(click.style("\n  Valid fiboa dataset", fg="green"))
    elif not issues:
        click.echo(click.style(f"\n  Valid with {len(warnings)} warning(s)", fg="green"))
    else:
        click.echo(click.style(f"\n  INVALID: {len(issues)} error(s)", fg="red"))
