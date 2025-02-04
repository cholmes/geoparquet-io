#!/usr/bin/env python3

import click
import duckdb
from geoparquet_tools.core.common import (
    safe_file_url, find_primary_geometry_column, get_parquet_metadata,
    update_metadata
)

def add_h3(input_parquet, output_parquet, resolution=15, column_name='h3-cell', verbose=False):
    """Add H3 cell IDs to a GeoParquet file based on geometry centroids."""
    # Get safe URL for input file
    input_url = safe_file_url(input_parquet, verbose)
    
    # Get metadata before processing
    metadata, _ = get_parquet_metadata(input_parquet, verbose)
    
    # Get geometry column name
    geometry_column = find_primary_geometry_column(input_parquet, verbose)
    if verbose:
        click.echo(f"Using geometry column: {geometry_column}")
    
    # Create DuckDB connection and load extensions
    con = duckdb.connect()
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("INSTALL h3 FROM community;")
    con.execute("LOAD h3;")
    
    if verbose:
        click.echo(f"Adding H3 cells at resolution {resolution}")
    
    # Build and execute query to add H3 cells
    query = f"""
    COPY (
        SELECT 
            *,
            h3_latlng_to_cell_string(
                ST_Y(ST_Centroid({geometry_column})), 
                ST_X(ST_Centroid({geometry_column})), 
                {resolution}
            ) AS "{column_name}"
        FROM '{input_url}'
    )
    TO '{output_parquet}'
    (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    
    con.execute(query)
    
    # Update metadata
    if metadata:
        update_metadata(output_parquet, metadata)
        if verbose:
            click.echo("Updated output file with original metadata")
    
    if verbose:
        # Get count of features processed
        count = con.execute(f"SELECT COUNT(*) FROM '{output_parquet}'").fetchone()[0]
        click.echo(f"Added H3 cells to {count:,} features")
    
    click.echo(f"Successfully wrote output to: {output_parquet}")

if __name__ == "__main__":
    add_h3() 