#!/usr/bin/env python3

import os
import click
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, get_parquet_metadata, update_metadata
)

def partition_by_h3(input_parquet, output_folder, column_name='h3-cell', 
                   resolution=8, dry_run=False, verbose=False):
    """
    Partition a GeoParquet file by H3 cells at specified resolution.
    
    Uses the first N characters of the H3 cell IDs to partition at the specified
    resolution. If the H3 column doesn't exist, suggests using 'gt add h3' first.
    In dry-run mode, reports statistics about the potential partitioning.
    """
    input_url = safe_file_url(input_parquet, verbose)
    
    # Create DuckDB connection
    con = duckdb.connect()
    
    # First verify the H3 column exists by checking schema
    query = f"SELECT * FROM '{input_url}' LIMIT 0"
    result = con.execute(query)
    columns = [desc[0] for desc in result.description]
    
    if column_name not in columns:
        raise click.ClickException(
            f"Column '{column_name}' not found. "
            f"Please add H3 cells first using 'gt add h3'"
        )
    
    # Get partition statistics using LEFT() to truncate H3 cells to resolution
    stats_query = f"""
    SELECT 
        COUNT(DISTINCT LEFT("{column_name}", {resolution})) as partition_count,
        COUNT(*) as total_rows,
        COUNT(*) * 1.0 / COUNT(DISTINCT LEFT("{column_name}", {resolution})) as avg_rows_per_partition
    FROM '{input_url}';
    """
    
    stats = con.execute(stats_query).fetchone()
    partition_count, total_rows, avg_rows = stats
    
    # Estimate file size per partition
    file_size = os.path.getsize(input_parquet)
    avg_partition_size = file_size * avg_rows / total_rows
    
    if verbose or dry_run:
        click.echo("\nPartitioning Statistics:")
        click.echo(f"Total rows: {total_rows:,}")
        click.echo(f"Number of partitions at resolution {resolution}: {partition_count:,}")
        click.echo(f"Average rows per partition: {avg_rows:.1f}")
        click.echo(f"Estimated average partition size: {avg_partition_size/1024/1024:.1f} MB")
    
    if dry_run:
        return
    
    # Create output directory
    os.makedirs(output_folder, exist_ok=True)
    if verbose:
        click.echo(f"\nCreated output directory: {output_folder}")
    
    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)
    
    # Get list of unique truncated H3 cells
    cells = con.execute(f"""
        SELECT DISTINCT LEFT("{column_name}", {resolution}) as h3_prefix
        FROM '{input_url}'
        ORDER BY h3_prefix;
    """).fetchall()
    
    total_cells = len(cells)
    
    for i, (cell_prefix,) in enumerate(cells, 1):
        if verbose:
            click.echo(f"Processing partition {i}/{total_cells}: {cell_prefix}")
        
        output_file = os.path.join(output_folder, f"{cell_prefix}.parquet")
        
        # Write partition - group all cells that share the same prefix
        query = f"""
        COPY (
            SELECT *
            FROM '{input_url}'
            WHERE LEFT("{column_name}", {resolution}) = '{cell_prefix}'
        )
        TO '{output_file}'
        (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        
        con.execute(query)
        
        # Update metadata for each partition
        if metadata:
            update_metadata(output_file, metadata)
    
    if verbose:
        click.echo(f"\nSuccessfully created {total_cells:,} partition files in {output_folder}") 