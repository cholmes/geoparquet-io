"""fiboa CLI commands for gpio."""

from __future__ import annotations

import click


@click.group()
def fiboa():
    """fiboa (Field Boundaries for Agriculture) commands.

    Tools for validating, improving, and inspecting GeoParquet files
    against the fiboa specification (https://fiboa.org).
    """


@fiboa.command()
@click.argument("input_file")
@click.option("-v", "--verbose", is_flag=True, help="Print verbose output")
def validate(input_file: str, verbose: bool) -> None:
    """Validate a GeoParquet file against the fiboa specification.

    Checks for required columns, correct data types, valid admin codes,
    and positive geometry metrics values.

    \b
    **Examples:**

    \b
    # Validate a fiboa dataset
    gpio fiboa validate fields.parquet

    \b
    # Verbose validation output
    gpio fiboa validate fields.parquet --verbose
    """
    from gpio_fiboa.validate import validate_fiboa

    validate_fiboa(input_file, verbose)


@fiboa.command()
@click.argument("input_file")
@click.argument("output_file")
@click.option(
    "-sz",
    "--geometry-metrics",
    is_flag=True,
    help="Add/recalculate metrics:area and metrics:perimeter columns.",
)
@click.option(
    "-a",
    "--admin",
    is_flag=True,
    help="Add admin:country_code and admin:subdivision_code columns via spatial join.",
)
@click.option(
    "-s",
    "--schemas",
    is_flag=True,
    help="Add/update the Vecorel schemas metadata with fiboa + extension URLs.",
)
@click.option(
    "--compression",
    type=click.Choice(
        ["ZSTD", "GZIP", "BROTLI", "LZ4", "SNAPPY", "UNCOMPRESSED"],
        case_sensitive=False,
    ),
    default="ZSTD",
    help="Compression type for output file (default: ZSTD)",
)
@click.option(
    "--compression-level",
    type=click.IntRange(1, 22),
    default=None,
    help="Compression level - GZIP: 1-9, ZSTD: 1-22, BROTLI: 1-11. Ignored for LZ4/SNAPPY.",
)
@click.option(
    "--row-group-size",
    type=int,
    default=None,
    help="Exact number of rows per row group (default: 100000)",
)
@click.option(
    "--row-group-size-mb",
    default=None,
    help="Target row group size (e.g. '256MB', '1GB', '128' assumes MB)",
)
@click.option(
    "--geoparquet-version",
    type=click.Choice(["1.0", "1.1", "2.0", "parquet-geo-only"]),
    default=None,
    help="GeoParquet version to write. Auto-detects from input if not specified.",
)
@click.option(
    "--determination-datetime",
    default=None,
    help="Source column name to rename, or ISO datetime literal "
    "(e.g., 'time' or '2024-01-01T00:00:00Z').",
)
@click.option(
    "--determination-method",
    type=click.Choice(
        ["manual", "surveyed", "driven", "auto-operation", "auto-imagery", "unknown"],
        case_sensitive=True,
    ),
    default=None,
    help="How boundaries were determined.",
)
@click.option(
    "--category",
    default=None,
    help="Comma-separated field categories: conceptual, operational, "
    "economic, administrative, other.",
)
@click.option(
    "--keep-source-columns",
    is_flag=True,
    help="Keep original columns when mapping to fiboa names "
    "(e.g., keep 'time' alongside 'determination:datetime').",
)
@click.option("--overwrite", is_flag=True, help="Overwrite existing output file")
@click.option("-v", "--verbose", is_flag=True, help="Print verbose output")
def improve(
    input_file: str,
    output_file: str,
    geometry_metrics: bool,
    admin: bool,
    schemas: bool,
    compression: str,
    compression_level: int | None,
    row_group_size: int | None,
    row_group_size_mb: str | None,
    geoparquet_version: str | None,
    determination_datetime: str | None,
    determination_method: str | None,
    category: str | None,
    keep_source_columns: bool,
    overwrite: bool,
    verbose: bool,
) -> None:
    """Improve a GeoParquet file for fiboa compliance.

    Apply multiple enhancements in sequence: geometry metrics, admin divisions,
    and schema metadata. Each flag enables a specific improvement.

    \b
    **Examples:**

    \b
    # Add geometry metrics (area + perimeter)
    gpio fiboa improve input.parquet output.parquet -sz

    \b
    # Add admin divisions + metrics
    gpio fiboa improve input.parquet output.parquet -sz -a

    \b
    # Full fiboa compliance pass
    gpio fiboa improve input.parquet output.parquet -sz -a -s
    """
    from gpio_fiboa.improve import VALID_CATEGORIES, improve_fiboa

    # Validate category values
    parsed_categories = None
    if category:
        parsed_categories = [c.strip() for c in category.split(",")]
        invalid = [c for c in parsed_categories if c not in VALID_CATEGORIES]
        if invalid:
            raise click.BadParameter(
                f"Invalid categories: {invalid}. Allowed: {', '.join(sorted(VALID_CATEGORIES))}",
                param_hint="--category",
            )

    # Parse row-group-size-mb if provided
    row_group_mb = None
    if row_group_size_mb is not None:
        from geoparquet_io.cli.decorators import parse_row_group_options

        row_group_mb = parse_row_group_options(row_group_size, row_group_size_mb)

    improve_fiboa(
        input_file,
        output_file,
        add_metrics=geometry_metrics,
        add_admin=admin,
        add_schemas=schemas,
        determination_datetime=determination_datetime,
        determination_method=determination_method,
        category=parsed_categories,
        keep_source_columns=keep_source_columns,
        compression=compression.upper(),
        compression_level=compression_level,
        row_group_size_mb=row_group_mb,
        row_group_rows=row_group_size,
        geoparquet_version=geoparquet_version,
        overwrite=overwrite,
        verbose=verbose,
    )


@fiboa.command()
@click.argument("input_file")
@click.option("-v", "--verbose", is_flag=True, help="Print verbose output")
def describe(input_file: str, verbose: bool) -> None:
    """Describe a GeoParquet file's fiboa compliance.

    Shows which fiboa/Vecorel extensions are present, collection metadata,
    and column coverage statistics.

    \b
    **Examples:**

    \b
    gpio fiboa describe fields.parquet
    """
    from gpio_fiboa.describe import describe_fiboa

    describe_fiboa(input_file, verbose)
