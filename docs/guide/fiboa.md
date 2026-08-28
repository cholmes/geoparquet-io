# fiboa Plugin

The `gpio-fiboa` plugin adds commands for working with field boundary datasets according to the [fiboa specification](https://fiboa.org) and [Vecorel](https://vecorel.org) encoding.

## Installation

=== "uv"
    <!-- doctest: skip="requires the gpio-fiboa plugin" -->
    ```bash
    uv tool install geoparquet-io --with gpio-fiboa
    ```

=== "pipx"
    <!-- doctest: skip="requires the gpio-fiboa plugin" -->
    ```bash
    pipx install geoparquet-io --preinstall gpio-fiboa
    ```

=== "Python project"
    <!-- doctest: skip="runs inside a plugin project the reader creates" -->
    ```bash
    uv add geoparquet-io gpio-fiboa
    ```

Verify the plugin is loaded:

<!-- doctest: skip="requires the gpio-fiboa plugin" -->
```bash
gpio fiboa --help
```

## Improve

Apply fiboa enhancements to a GeoParquet file. Each flag enables a specific improvement, applied in sequence.

=== "CLI"
    <!-- doctest: skip="requires the gpio-fiboa plugin" -->
    ```bash
    # Add geometry metrics (area + perimeter)
    gpio fiboa improve input.parquet output.parquet -sz

    # Add admin divisions via spatial join
    gpio fiboa improve input.parquet output.parquet -a

    # Full compliance pass: metrics + admin + schemas + Hilbert sorting
    gpio fiboa improve input.parquet output.parquet -sz -a -s

    # Map a datetime column and set determination method
    gpio fiboa improve input.parquet output.parquet \
        --determination-datetime time \
        --determination-method auto-imagery

    # Set category and keep original columns
    gpio fiboa improve input.parquet output.parquet \
        --category operational,economic \
        --keep-source-columns

    # Skip default Hilbert sorting
    gpio fiboa improve input.parquet output.parquet -sz --skip-hilbert
    ```

=== "Python"
    <!-- doctest: skip="requires the gpio-fiboa plugin" -->
    ```python
    from gpio_fiboa import improve_fiboa

    improve_fiboa(
        "input.parquet",
        "output.parquet",
        add_metrics=True,
        add_admin=True,
        add_schemas=True,
    )
    ```

### Improvement flags

| Flag | Description |
|------|-------------|
| `-sz` / `--geometry-metrics` | Add `metrics:area` and `metrics:perimeter` columns |
| `-a` / `--admin` | Add `admin:country_code` and `admin:subdivision_code` via Overture spatial join |
| `-s` / `--schemas` | Add/update Vecorel collection metadata with fiboa + extension schema URLs |
| `--determination-datetime` | Map an existing column or set a literal ISO datetime |
| `--determination-method` | Set how boundaries were determined (e.g., `auto-imagery`) |
| `--category` | Comma-separated field categories (`conceptual`, `operational`, `economic`, `administrative`, `other`) |
| `--keep-source-columns` | Preserve original columns when mapping to fiboa names |
| `--skip-hilbert` | Skip Hilbert spatial sorting (enabled by default) |

### Output options

Standard gpio output options are available: `--compression`, `--compression-level`, `--row-group-size`, `--row-group-size-mb`, `--geoparquet-version`, `--overwrite`.

### GeoParquet 2.0 handling

If the input file uses GeoParquet 2.0 or native Parquet geo types, `improve` automatically downgrades to GeoParquet 1.1 for Vecorel/fiboa compatibility. Use `--geoparquet-version 2.0` to override.

## Describe

Show fiboa column coverage, Vecorel extension metadata, and summary statistics.

=== "CLI"
    <!-- doctest: skip="requires the gpio-fiboa plugin" -->
    ```bash
    gpio fiboa describe fields.parquet

    # Include non-fiboa columns in output
    gpio fiboa describe fields.parquet --verbose
    ```

=== "Python"
    <!-- doctest: skip="requires the gpio-fiboa plugin" -->
    ```python
    from gpio_fiboa import describe_fiboa

    describe_fiboa("fields.parquet", verbose=True)
    ```

The describe command shows:

- Vecorel metadata and detected extensions
- Present and missing fiboa columns
- Metrics summary (min/max/mean for area and perimeter)
- Admin coverage (unique country codes)

## Common workflows

### Full compliance from raw data

<!-- doctest: skip="requires the gpio-fiboa plugin" -->
```bash
gpio fiboa improve raw.parquet compliant.parquet -sz -a -s \
    --determination-datetime acquisition_date \
    --determination-method auto-imagery \
    --category operational

gpio fiboa describe compliant.parquet
```

### Add schemas to an already-enriched file

<!-- doctest: skip="requires the gpio-fiboa plugin" -->
```bash
gpio fiboa improve enriched.parquet output.parquet -s --skip-hilbert
```
