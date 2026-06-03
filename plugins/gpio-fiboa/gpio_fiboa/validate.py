"""Validate GeoParquet files against the fiboa specification."""

from __future__ import annotations

import json
import re
import sys

import pyarrow.parquet as pq

FIBOA_SCHEMA_URL = "https://fiboa.org/specification/v0.3.0/schema.yaml"
VECOREL_CORE_URL = "https://vecorel.org/specification/v0.1.0/schema.yaml"

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

VALID_CATEGORIES = {
    "conceptual",
    "operational",
    "economic",
    "administrative",
    "other",
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
        _print_result(f"Cannot read file: {e}", [], [], [])
        return False

    schema = pf.schema_arrow
    col_names = set(schema.names)
    metadata = schema.metadata or {}

    # Check geometry column
    if "geometry" not in col_names:
        issues.append("Missing required column: geometry")

    # Check for fiboa-specific columns and report coverage
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

    # Check Collection metadata
    collection_meta = metadata.get(b"collection")
    if collection_meta:
        try:
            vecorel = json.loads(collection_meta)
            schema_urls = vecorel.get("schemas", {}).get("default", [])
            if FIBOA_SCHEMA_URL not in schema_urls:
                warnings.append(
                    f"Collection schemas metadata missing fiboa URL: {FIBOA_SCHEMA_URL}"
                )
            if verbose:
                info_msgs.append(f"Collection schemas: {schema_urls}")
        except json.JSONDecodeError:
            issues.append("Collection metadata is not valid JSON")
    else:
        warnings.append("No collection metadata found (missing 'collection' key in file metadata)")

    # Validate data values (sample first row group)
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
    # Check admin:country_code pattern
    if "admin:country_code" in col_names:
        col = table.column("admin:country_code")
        invalid_count = 0
        for val in col:
            if val.is_valid and not COUNTRY_CODE_PATTERN.match(str(val)):
                invalid_count += 1
        if invalid_count > 0:
            issues.append(
                f"admin:country_code has {invalid_count} values not matching "
                f"ISO 3166-1 alpha-2 pattern (^[A-Z]{{2}}$)"
            )

    # Check admin:subdivision_code pattern
    if "admin:subdivision_code" in col_names:
        col = table.column("admin:subdivision_code")
        invalid_count = 0
        for val in col:
            if val.is_valid and not SUBDIVISION_CODE_PATTERN.match(str(val)):
                invalid_count += 1
        if invalid_count > 0:
            warnings.append(
                f"admin:subdivision_code has {invalid_count} values not matching "
                f"ISO 3166-2 pattern (^[A-Z0-9]{{1,3}}$)"
            )

    # Check metrics:area > 0
    if "metrics:area" in col_names:
        col = table.column("metrics:area")
        import pyarrow.compute as pc

        non_null = pc.filter(col, pc.is_valid(col))
        if len(non_null) > 0:
            min_val = pc.min(non_null).as_py()
            if min_val is not None and min_val <= 0:
                issues.append(f"metrics:area has values <= 0 (min: {min_val})")

    # Check metrics:perimeter > 0
    if "metrics:perimeter" in col_names:
        col = table.column("metrics:perimeter")
        import pyarrow.compute as pc

        non_null = pc.filter(col, pc.is_valid(col))
        if len(non_null) > 0:
            min_val = pc.min(non_null).as_py()
            if min_val is not None and min_val <= 0:
                issues.append(f"metrics:perimeter has values <= 0 (min: {min_val})")

    # Check determination:method values
    if "determination:method" in col_names:
        col = table.column("determination:method")
        invalid_values = set()
        for val in col:
            if val.is_valid and str(val) not in VALID_DETERMINATION_METHODS:
                invalid_values.add(str(val))
        if invalid_values:
            warnings.append(f"determination:method has non-standard values: {invalid_values}")


def _print_result(
    input_file: str,
    issues: list[str],
    warnings: list[str],
    info_msgs: list[str],
) -> None:
    """Print validation results."""
    if isinstance(input_file, str) and not input_file.startswith("Cannot"):
        print(f"\nfiboa validation: {input_file}")
        print("=" * 60)

    if issues:
        print(f"\nErrors ({len(issues)}):")
        for issue in issues:
            print(f"  x {issue}")

    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for warning in warnings:
            print(f"  ! {warning}")

    if info_msgs:
        print("\nInfo:")
        for msg in info_msgs:
            print(f"  - {msg}")

    if not issues and not warnings:
        print("\n  Valid fiboa dataset")
    elif not issues:
        print(f"\n  Valid with {len(warnings)} warning(s)")
    else:
        print(f"\n  INVALID: {len(issues)} error(s)")
        sys.exit(1)
