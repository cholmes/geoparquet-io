"""
Pure table-centric operations for GeoParquet transformations.

These functions accept and return PyArrow Tables, making them easy to
compose and integrate with other Arrow-based workflows.

Example:
    import pyarrow.parquet as pq
    from geoparquet_io.api import ops

    table = pq.read_table('input.parquet')
    table = ops.add_bbox(table)
    table = ops.add_quadkey(table, resolution=12)
    table = ops.sort_hilbert(table)
    pq.write_table(table, 'output.parquet')
"""

from __future__ import annotations

import pyarrow as pa

from geoparquet_io.core.add.a5 import add_a5_table
from geoparquet_io.core.add.bbox import add_bbox_table
from geoparquet_io.core.add.h3 import add_h3_table
from geoparquet_io.core.add.kdtree import add_kdtree_table
from geoparquet_io.core.add.quadkey import add_quadkey_table
from geoparquet_io.core.add.s2 import add_s2_table
from geoparquet_io.core.extract import extract_table
from geoparquet_io.core.hilbert_order import hilbert_order_table
from geoparquet_io.core.reproject import reproject_table
from geoparquet_io.core.sort_by_column import sort_by_column_table
from geoparquet_io.core.sort_quadkey import sort_by_quadkey_table


def add_bbox(
    table: pa.Table,
    column_name: str = "bbox",
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a bounding box struct column to a table.

    Args:
        table: Input PyArrow Table
        column_name: Name for the bbox column (default: 'bbox')
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with bbox column added
    """
    return add_bbox_table(
        table,
        bbox_column_name=column_name,
        geometry_column=geometry_column,
    )


def add_quadkey(
    table: pa.Table,
    column_name: str = "quadkey",
    resolution: int = 13,
    use_centroid: bool = False,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a quadkey column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the quadkey column (default: 'quadkey')
        resolution: Quadkey zoom level 0-23 (default: 13)
        use_centroid: Force centroid even if bbox exists
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with quadkey column added
    """
    return add_quadkey_table(
        table,
        quadkey_column_name=column_name,
        resolution=resolution,
        use_centroid=use_centroid,
        geometry_column=geometry_column,
    )


def sort_hilbert(
    table: pa.Table,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reorder table rows using Hilbert curve ordering.

    Args:
        table: Input PyArrow Table
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with rows reordered by Hilbert curve
    """
    return hilbert_order_table(
        table,
        geometry_column=geometry_column,
    )


def extract(
    table: pa.Table,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    bbox: tuple[float, float, float, float] | None = None,
    where: str | None = None,
    limit: int | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Extract columns and rows with optional filtering.

    Args:
        table: Input PyArrow Table
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        bbox: Bounding box filter (xmin, ymin, xmax, ymax)
        where: SQL WHERE clause
        limit: Maximum rows to return
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        Filtered table
    """
    return extract_table(
        table,
        columns=columns,
        exclude_columns=exclude_columns,
        bbox=bbox,
        where=where,
        limit=limit,
        geometry_column=geometry_column,
    )


def add_h3(
    table: pa.Table,
    column_name: str = "h3_cell",
    resolution: int = 9,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an H3 cell column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the H3 column (default: 'h3_cell')
        resolution: H3 resolution level 0-15 (default: 9)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with H3 column added
    """
    return add_h3_table(
        table,
        h3_column_name=column_name,
        resolution=resolution,
        geometry_column=geometry_column,
    )


def add_a5(
    table: pa.Table,
    column_name: str = "a5_cell",
    resolution: int = 15,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an A5 cell column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the A5 column (default: 'a5_cell')
        resolution: A5 resolution level 0-30 (default: 15)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with A5 column added
    """
    return add_a5_table(
        table,
        a5_column_name=column_name,
        resolution=resolution,
        geometry_column=geometry_column,
    )


def add_kdtree(
    table: pa.Table,
    column_name: str = "kdtree_cell",
    iterations: int = 9,
    sample_size: int = 100000,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add a KD-tree cell column based on geometry location.

    Args:
        table: Input PyArrow Table
        column_name: Name for the KD-tree column (default: 'kdtree_cell')
        iterations: Number of recursive splits 1-20 (default: 9)
        sample_size: Number of points to sample for boundaries (default: 100000)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with KD-tree column added
    """
    return add_kdtree_table(
        table,
        kdtree_column_name=column_name,
        iterations=iterations,
        sample_size=sample_size,
        geometry_column=geometry_column,
    )


def add_s2(
    table: pa.Table,
    column_name: str = "s2_cell",
    level: int = 13,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Add an S2 cell column based on geometry location.

    Uses Google's S2 spherical geometry library to compute cell IDs
    from geometry centroids. Cell IDs are stored as hex tokens for portability.

    Args:
        table: Input PyArrow Table
        column_name: Name for the S2 column (default: 's2_cell')
        level: S2 level 0-30 (default: 13, ~1.2 km² cells)
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with S2 column added

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = pq.read_table('input.parquet')
        >>> table = ops.add_s2(table, level=13)
        >>> pq.write_table(table, 'output.parquet')
    """
    return add_s2_table(
        table,
        s2_column_name=column_name,
        level=level,
        geometry_column=geometry_column,
    )


def sort_column(
    table: pa.Table,
    column: str | list[str],
    descending: bool = False,
) -> pa.Table:
    """
    Sort table rows by the specified column(s).

    Args:
        table: Input PyArrow Table
        column: Column name or list of column names to sort by
        descending: Sort in descending order (default: False)

    Returns:
        New table with rows sorted by the column(s)
    """
    return sort_by_column_table(
        table,
        columns=column,
        descending=descending,
    )


def sort_quadkey(
    table: pa.Table,
    column_name: str = "quadkey",
    resolution: int = 13,
    use_centroid: bool = False,
    remove_column: bool = False,
) -> pa.Table:
    """
    Sort table rows by quadkey column.

    If the quadkey column doesn't exist, it will be auto-added.

    Args:
        table: Input PyArrow Table
        column_name: Name of the quadkey column (default: 'quadkey')
        resolution: Quadkey resolution for auto-adding (0-23, default: 13)
        use_centroid: Use geometry centroid when auto-adding
        remove_column: Remove the quadkey column after sorting

    Returns:
        New table with rows sorted by quadkey
    """
    return sort_by_quadkey_table(
        table,
        quadkey_column_name=column_name,
        resolution=resolution,
        use_centroid=use_centroid,
        remove_quadkey_column=remove_column,
    )


def reproject(
    table: pa.Table,
    target_crs: str = "EPSG:4326",
    source_crs: str | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """
    Reproject geometry to a different coordinate reference system.

    Args:
        table: Input PyArrow Table
        target_crs: Target CRS (default: EPSG:4326)
        source_crs: Source CRS. If None, detected from metadata.
        geometry_column: Geometry column name (auto-detected if None)

    Returns:
        New table with reprojected geometry
    """
    return reproject_table(
        table,
        target_crs=target_crs,
        source_crs=source_crs,
        geometry_column=geometry_column,
    )


def read_bigquery(
    table_id: str,
    *,
    project: str | None = None,
    credentials_file: str | None = None,
    where: str | None = None,
    bbox: str | None = None,
    bbox_mode: str = "auto",
    bbox_threshold: int = 500000,
    limit: int | None = None,
    columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    geography_column: str | None = None,
    geometry_format: str = "wkt",
    edges: str | None = None,
) -> pa.Table:
    """
    Read data from a BigQuery table.

    Uses DuckDB's BigQuery extension with the Storage Read API for
    efficient Arrow-based scanning with filter pushdown.

    Native GEOGRAPHY columns are automatically detected and converted
    with spherical edges. VARCHAR columns containing WKT or GeoJSON
    can be parsed by specifying geography_column and geometry_format.

    Args:
        table_id: Fully qualified BigQuery table ID (project.dataset.table)
        project: GCP project ID (overrides project in table_id if set)
        credentials_file: Path to service account JSON file
        where: SQL WHERE clause for filtering (BigQuery SQL syntax)
        bbox: Bounding box for spatial filter as "minx,miny,maxx,maxy"
        bbox_mode: Filtering mode - "auto" (default), "server", or "local"
        bbox_threshold: Row count threshold for auto mode (default: 500000)
        limit: Maximum rows to extract
        columns: Columns to include (None = all)
        exclude_columns: Columns to exclude
        geography_column: Column containing geometry data. Auto-detected
            for native GEOGRAPHY columns. Specify to parse VARCHAR columns.
        geometry_format: Format of VARCHAR geometry ("wkt" or "geojson")
        edges: Edge interpretation ("spherical" or "planar"). Native
            GEOGRAPHY columns default to "spherical", VARCHAR to "planar".

    Returns:
        PyArrow Table with BigQuery data

    Raises:
        FileNotFoundError: If credentials_file doesn't exist
        RuntimeError: If BigQuery query fails

    Note:
        **Cannot read BigQuery views or external tables** - this is a
        limitation of the BigQuery Storage Read API.

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.read_bigquery('myproject.geodata.buildings')
        >>> table = ops.add_bbox(table)
        >>> pq.write_table(table, 'output.parquet')

        >>> # Parse VARCHAR column as WKT geometry
        >>> table = ops.read_bigquery(
        ...     'myproject.dataset.table',
        ...     geography_column='geometry',
        ...     geometry_format='wkt'
        ... )
    """
    from geoparquet_io.core.extract_bigquery import extract_bigquery

    # Convert columns list to comma-separated string for the core function
    include_cols = ",".join(columns) if columns else None
    exclude_cols = ",".join(exclude_columns) if exclude_columns else None

    # Validate bbox_mode
    valid_bbox_modes = {"auto", "server", "local"}
    if bbox_mode not in valid_bbox_modes:
        raise ValueError(
            f"Invalid bbox_mode '{bbox_mode}' for table '{table_id}'. "
            f"Must be one of: {', '.join(sorted(valid_bbox_modes))}"
        )

    # Validate bbox_threshold
    if not isinstance(bbox_threshold, int) or bbox_threshold < 0:
        raise ValueError(
            f"Invalid bbox_threshold '{bbox_threshold}' for table '{table_id}'. "
            "Must be an integer >= 0."
        )

    # Get PyArrow table (don't write to file)
    arrow_table = extract_bigquery(
        table_id=table_id,
        output_parquet=None,  # Return table instead of writing
        project=project,
        credentials_file=credentials_file,
        where=where,
        bbox=bbox,
        bbox_mode=bbox_mode,
        bbox_threshold=bbox_threshold,
        limit=limit,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        geography_column=geography_column,
        geometry_format=geometry_format,
        edges=edges,
        verbose=False,
    )

    if arrow_table is None:
        raise RuntimeError(f"Failed to read from BigQuery table: {table_id}")

    return arrow_table


def convert_to_geojson(
    table: pa.Table,
    output_path: str | None = None,
    rs: bool = True,
    precision: int = 7,
    write_bbox: bool = False,
    id_field: str | None = None,
) -> str | None:
    """
    Convert a GeoParquet table to GeoJSON.

    Writes to file if output_path is provided, otherwise streams to stdout.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path, or None to stream to stdout
        rs: Include RFC 8142 record separators (streaming mode only)
        precision: Coordinate decimal precision (default 7 per RFC 7946).
            Note: Very low precision values (e.g., 3) may collapse small
            geometries since coordinates are snapped to a grid.
        write_bbox: Include bbox property for each feature
        id_field: Field to use as feature 'id' member

    Returns:
        Output path if writing to file, None if streaming to stdout
    """
    import tempfile
    import uuid
    from pathlib import Path

    from geoparquet_io.core.geojson_stream import (
        convert_to_geojson as convert_to_geojson_impl,
    )

    if not isinstance(table, pa.Table):
        raise TypeError(f"Expected pa.Table, got {type(table).__name__}")

    # Write table to temp parquet file for processing
    temp_dir = Path(tempfile.gettempdir())
    temp_input = temp_dir / f"gpio_geojson_{uuid.uuid4()}.parquet"

    try:
        import pyarrow.parquet as pq

        pq.write_table(table, str(temp_input))

        # Call core function
        convert_to_geojson_impl(
            input_path=str(temp_input),
            output_path=output_path,
            rs=rs,
            precision=precision,
            write_bbox=write_bbox,
            id_field=id_field,
        )

        return output_path

    finally:
        # Clean up temp file
        if temp_input.exists():
            temp_input.unlink()


def _table_to_temp_parquet_and_convert(
    table: pa.Table,
    output_path: str,
    writer_func,
    prefix: str,
    **writer_kwargs,
) -> str:
    """
    Helper function to convert PyArrow Table to a format via temp parquet file.

    Eliminates repetitive temp file handling across all conversion functions.

    Args:
        table: Input PyArrow Table
        output_path: Output file path
        writer_func: Writer function from format_writers module
        prefix: Prefix for temp file name
        **writer_kwargs: Keyword arguments to pass to writer function

    Returns:
        Output path

    Raises:
        TypeError: If table is not a PyArrow Table
    """
    import tempfile
    import uuid
    from pathlib import Path

    if not isinstance(table, pa.Table):
        raise TypeError(f"Expected pa.Table, got {type(table).__name__}")

    # Write table to temp parquet file for processing
    temp_dir = Path(tempfile.gettempdir())
    temp_input = temp_dir / f"gpio_{prefix}_{uuid.uuid4()}.parquet"

    try:
        import pyarrow.parquet as pq

        pq.write_table(table, str(temp_input))

        # Call writer function
        writer_func(
            input_path=str(temp_input),
            output_path=output_path,
            verbose=False,
            **writer_kwargs,
        )

        return output_path

    finally:
        # Clean up temp file
        if temp_input.exists():
            temp_input.unlink()


def convert_to_geopackage(
    table: pa.Table,
    output_path: str,
    overwrite: bool = False,
    layer_name: str = "features",
) -> str:
    """
    Convert a GeoParquet table to GeoPackage format.

    Writes to file and creates spatial index automatically.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)
        overwrite: Overwrite existing file (default: False)
        layer_name: Layer name in GeoPackage (default: 'features')

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_geopackage

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_geopackage,
        "geopackage",
        overwrite=overwrite,
        layer_name=layer_name,
    )


def convert_to_flatgeobuf(
    table: pa.Table,
    output_path: str,
) -> str:
    """
    Convert a GeoParquet table to FlatGeobuf format.

    Writes to file and creates spatial index automatically.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_flatgeobuf

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_flatgeobuf,
        "flatgeobuf",
    )


def convert_to_csv(
    table: pa.Table,
    output_path: str,
    include_wkt: bool = True,
    include_bbox: bool = True,
) -> str:
    """
    Convert a GeoParquet table to CSV format.

    Converts geometry to WKT text representation.
    Complex types (STRUCT, LIST, MAP) are JSON-encoded.

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)
        include_wkt: Include WKT geometry column (default: True)
        include_bbox: Include bbox column if present (default: True)

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_csv

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_csv,
        "csv",
        include_wkt=include_wkt,
        include_bbox=include_bbox,
    )


def convert_to_shapefile(
    table: pa.Table,
    output_path: str,
    overwrite: bool = False,
    encoding: str = "UTF-8",
) -> str:
    """
    Convert a GeoParquet table to Shapefile format.

    Note: Shapefiles have significant limitations:
    - Column names truncated to 10 characters
    - File size limit of 2GB
    - Limited data type support
    - Creates multiple files (.shp, .shx, .dbf, .prj)

    Args:
        table: Input PyArrow Table with geometry column
        output_path: Output file path (must be local, not cloud URL)
        overwrite: Overwrite existing file (default: False)
        encoding: Character encoding (default: 'UTF-8')

    Returns:
        Output path
    """
    from geoparquet_io.core.format_writers import write_shapefile

    return _table_to_temp_parquet_and_convert(
        table,
        output_path,
        write_shapefile,
        "shapefile",
        overwrite=overwrite,
        encoding=encoding,
    )


def from_arcgis(
    service_url: str,
    token: str | None = None,
    where: str = "1=1",
    bbox: tuple[float, float, float, float] | None = None,
    include_cols: str | None = None,
    exclude_cols: str | None = None,
    limit: int | None = None,
    max_workers: int = 1,
) -> pa.Table:
    """
    Fetch ArcGIS Feature Service as a PyArrow Table.

    Lower-level function for users who want direct Arrow table access.
    Supports server-side filtering for efficient data transfer.

    Args:
        service_url: ArcGIS Feature Service URL with layer ID
        token: Optional authentication token
        where: SQL WHERE clause to filter features (default: "1=1" = all)
        bbox: Bounding box filter (xmin, ymin, xmax, ymax) in WGS84
        include_cols: Comma-separated column names to include (server-side)
        exclude_cols: Comma-separated column names to exclude (client-side)
        limit: Maximum number of features to return
        max_workers: Number of concurrent requests (1 = sequential, 2-3 recommended)

    Returns:
        PyArrow Table with WKB geometry column

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.from_arcgis('https://services.arcgis.com/.../FeatureServer/0')
        >>> table = ops.add_bbox(table)
        >>> table = ops.sort_hilbert(table)
        >>>
        >>> # With server-side filtering
        >>> table = ops.from_arcgis(url, bbox=(-122.5, 37.5, -122.0, 38.0), limit=1000)
        >>>
        >>> # With parallel fetching for large datasets
        >>> table = ops.from_arcgis(url, limit=100000, max_workers=3)
    """
    from geoparquet_io.core.arcgis import ArcGISAuth, arcgis_to_table

    auth = ArcGISAuth(token=token) if token else None
    return arcgis_to_table(
        service_url,
        auth=auth,
        where=where,
        bbox=bbox,
        include_cols=include_cols,
        exclude_cols=exclude_cols,
        limit=limit,
        max_workers=max_workers,
        verbose=False,
    )


def from_wfs(
    service_url: str,
    typename: str,
    version: str = "auto",
    bbox: tuple[float, float, float, float] | None = None,
    limit: int | None = None,
    max_workers: int = 1,
    page_size: int = 10000,
    axis_order: str = "auto",
    strict_crs: bool = False,
    auto_tile: bool = False,
) -> pa.Table:
    """
    Fetch WFS layer as PyArrow Table.

    Uses DuckDB's native HTTP streaming for fast extraction. For very large
    datasets (1M+ features), use max_workers > 1 to enable parallel pagination.

    Args:
        service_url: WFS service URL
        typename: Feature type name (e.g., 'cities' or 'ns:cities')
        version: WFS version ('auto', '2.0.0', '1.1.0', '1.0.0'). Default 'auto'
            tries 2.0.0, then 1.1.0, then 1.0.0.
        bbox: Optional bounding box filter (xmin, ymin, xmax, ymax)
        limit: Maximum features to fetch
        max_workers: Parallel requests for large datasets (default: 1)
        page_size: Features per page when using parallel mode (default: 10000)
        axis_order: Bbox axis order ('auto', 'xy', 'latlon'). 'auto' detects from
            CRS format - URN CRS with WFS 1.1.0+ uses lat,lon per OGC spec.
        strict_crs: If True, fail when server returns coordinates that don't match
            requested CRS. If False (default), warn and use detected CRS.
        auto_tile: Automatically subdivide into spatial tiles for servers with
            startIndex limits (default: False)

    Returns:
        PyArrow Table with geometry column

    Example:
        >>> from geoparquet_io.api import ops
        >>> table = ops.from_wfs('https://geo.example.com/wfs', 'cities', limit=100)
        >>> # For large datasets on servers with startIndex limits:
        >>> table = ops.from_wfs('https://geo.example.com/wfs', 'parcels', auto_tile=True)
    """
    from geoparquet_io.core.wfs import wfs_to_table

    return wfs_to_table(
        service_url,
        typename,
        version=version,
        bbox=bbox,
        limit=limit,
        max_workers=max_workers,
        page_size=page_size,
        axis_order=axis_order,
        strict_crs=strict_crs,
        auto_tile=auto_tile,
    )


def get_row_group_geo_stats(parquet_file: str) -> list[dict]:
    """
    Get per-row-group geo_bbox statistics from a GeoParquet file.

    Returns a list of dicts with row_group_id, num_rows, xmin, ymin,
    xmax, ymax for each row group. Useful for verifying spatial locality
    after Hilbert sorting.

    Tries native Parquet geo stats first (GeoParquet 2.0 / parquet-geo-only),
    then falls back to bbox column statistics if no native stats are available.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        List of dicts with per-row-group bbox statistics.
        Empty list if no geo stats are available.
    """
    from geoparquet_io.core.duckdb_metadata import (
        get_file_metadata,
        get_per_row_group_bbox_stats,
        get_per_row_group_native_geo_stats,
        has_bbox_column,
    )
    from geoparquet_io.core.file_utils import safe_file_url
    from geoparquet_io.core.metadata_utils import (
        _get_num_rows_per_row_group,
        _merge_row_counts,
    )

    safe_url = safe_file_url(parquet_file, verbose=False)

    # Try native geo stats first (GeoParquet 2.0 / parquet-geo-only)
    rg_stats = get_per_row_group_native_geo_stats(safe_url)

    # Fall back to bbox column if no native stats
    if not rg_stats:
        has_bbox, bbox_col_name = has_bbox_column(safe_url)
        if has_bbox and bbox_col_name:
            rg_stats = get_per_row_group_bbox_stats(safe_url, bbox_col_name)

    if not rg_stats:
        return []

    file_meta = get_file_metadata(safe_url)
    num_rows_per_rg = _get_num_rows_per_row_group(safe_url, file_meta)

    return _merge_row_counts(rg_stats, num_rows_per_rg)


def compression_stats(path: str) -> list[dict]:
    """
    Get per-column compression ratios for a Parquet file.

    Queries Parquet metadata to compute compressed and uncompressed sizes
    per column, along with the compression ratio.

    Args:
        path: Path or URL to the Parquet file

    Returns:
        List of dicts ordered by compressed_bytes descending, each with:
        - column_name: Column path in schema
        - compression: Compression codec name
        - compressed_bytes: Total compressed size in bytes
        - uncompressed_bytes: Total uncompressed size in bytes
        - ratio: Compression ratio (uncompressed / compressed)

    Example:
        >>> from geoparquet_io.api import ops
        >>> stats = ops.compression_stats('data.parquet')
        >>> for col in stats:
        ...     print(f"{col['column_name']}: {col['ratio']}x")
    """
    from geoparquet_io.core.duckdb_metadata import get_compression_stats

    return get_compression_stats(str(path))


def explain_analyze(
    file_path: str,
    query: str | None = None,
) -> dict:
    """
    Run EXPLAIN ANALYZE on a DuckDB query against a Parquet file.

    Shows per-operator timing, cardinality, filter pushdown detection,
    and row group pruning analysis.

    Args:
        file_path: Path to the input Parquet file.
        query: Optional SQL query. Use {file} as placeholder for the file path.
               Defaults to SELECT * FROM read_parquet('{file}').

    Returns:
        Dictionary with operators, timing, and analysis results.

    Example:
        >>> from geoparquet_io.api import ops
        >>> result = ops.explain_analyze('input.parquet')
        >>> for op in result['operators']:
        ...     print(f"{op['name']}: {op['timing']:.6f}s")
        >>> # With a custom query:
        >>> result = ops.explain_analyze(
        ...     'input.parquet',
        ...     query="SELECT * FROM read_parquet('{file}') WHERE id > 10"
        ... )
    """
    from geoparquet_io.core.benchmark import explain_analyze as _explain_analyze

    return _explain_analyze(
        file_path=file_path,
        query=query,
    )


def create_pmtiles(
    input_path: str,
    output_path: str,
    *,
    layer: str | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    verbose: bool = False,
    profile: str | None = None,
    src_crs: str | None = None,
    attribution: str | None = None,
    layer_by_column: str | None = None,
) -> None:
    """
    Create PMTiles from a GeoParquet file using tippecanoe.

    Streams GeoParquet through gpio and tippecanoe to generate PMTiles.
    Requires tippecanoe to be installed and available in PATH.

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path for output PMTiles file
        layer: Layer name in PMTiles (defaults to output filename)
        min_zoom: Minimum zoom level (optional)
        max_zoom: Maximum zoom level (optional, auto-detected if not set)
        bbox: Bounding box filter as "minx,miny,maxx,maxy"
        where: SQL WHERE clause for filtering
        include_cols: Comma-separated list of columns to include
        precision: Coordinate decimal precision (default: 6)
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        src_crs: Source CRS for reprojection to WGS84
        attribution: Attribution HTML for the tiles
        layer_by_column: Split tiles into layers grouped by the values of this column

    Raises:
        TippecanoeNotFoundError: If tippecanoe is not in PATH
        ValueError: If paths contain shell metacharacters
        RuntimeError: If any subprocess fails

    Example:
        >>> from geoparquet_io.api import ops
        >>> ops.create_pmtiles('buildings.parquet', 'buildings.pmtiles')
        >>> # With options:
        >>> ops.create_pmtiles(
        ...     'data.parquet',
        ...     'tiles.pmtiles',
        ...     layer='buildings',
        ...     max_zoom=14,
        ...     bbox='-122.5,37.5,-122.0,38.0'
        ... )
    """
    from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

    create_pmtiles_from_geoparquet(
        input_path=input_path,
        output_path=output_path,
        layer=layer,
        min_zoom=min_zoom,
        max_zoom=max_zoom,
        bbox=bbox,
        where=where,
        include_cols=include_cols,
        precision=precision,
        verbose=verbose,
        profile=profile,
        src_crs=src_crs,
        attribution=attribution,
        layer_by_column=layer_by_column,
    )
