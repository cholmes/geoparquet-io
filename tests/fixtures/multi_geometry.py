"""Fixtures for testing multi-geometry column support."""

import json

import pyarrow as pa
import pyarrow.parquet as pq


def create_multi_geometry_geoparquet(output_path: str) -> str:
    """Create a GeoParquet file with two geometry columns.

    Creates a file with:
    - id: integer
    - name: string
    - geometry: Point (primary) - location
    - boundary: Polygon (secondary) - bounding area

    Returns path to created file.
    """
    import duckdb

    # Use DuckDB to generate proper WKB bytes
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Generate WKB for points
    point_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()
        point_wkbs.append(result[0])

    # Generate WKB for polygons (1x1 boxes around each point)
    polygon_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        wkt = f"POLYGON(({x - 0.5} {y - 0.5}, {x + 0.5} {y - 0.5}, {x + 0.5} {y + 0.5}, {x - 0.5} {y + 0.5}, {x - 0.5} {y - 0.5}))"
        result = con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()
        polygon_wkbs.append(result[0])

    con.close()

    # Create Arrow table
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["A", "B", "C"], type=pa.string()),
            "geometry": pa.array(point_wkbs, type=pa.binary()),
            "boundary": pa.array(polygon_wkbs, type=pa.binary()),
        }
    )

    # GeoParquet metadata with two geometry columns
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
                "bbox": [0.0, 0.0, 2.0, 2.0],
            },
            "boundary": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
                "bbox": [-0.5, -0.5, 2.5, 2.5],
            },
        },
    }

    # Write with metadata
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path


def create_multi_geometry_geoparquet_different_crs(output_path: str) -> str:
    """Create a GeoParquet with two geometry columns having different CRS.

    - geometry: Point in EPSG:4326 (WGS84)
    - boundary: Polygon in EPSG:3857 (Web Mercator)

    Returns path to created file.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Points in WGS84
    point_wkbs = []
    for x, y in [(0, 0), (1, 1), (2, 2)]:
        result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y}))").fetchone()
        point_wkbs.append(result[0])

    # Polygons in Web Mercator coordinates (rough equivalent)
    polygon_wkbs = []
    for x, y in [(0, 0), (111319, 111325), (222638, 222684)]:
        wkt = f"POLYGON(({x - 50000} {y - 50000}, {x + 50000} {y - 50000}, {x + 50000} {y + 50000}, {x - 50000} {y + 50000}, {x - 50000} {y - 50000}))"
        result = con.execute(f"SELECT ST_AsWKB(ST_GeomFromText('{wkt}'))").fetchone()
        polygon_wkbs.append(result[0])

    con.close()

    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "geometry": pa.array(point_wkbs, type=pa.binary()),
            "boundary": pa.array(polygon_wkbs, type=pa.binary()),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": {"authority": "EPSG", "code": 4326},
                },
            },
            "boundary": {
                "encoding": "WKB",
                "geometry_types": ["Polygon"],
                "crs": {
                    "$schema": "https://proj.org/schemas/v0.7/projjson.schema.json",
                    "type": "ProjectedCRS",
                    "name": "WGS 84 / Pseudo-Mercator",
                    "id": {"authority": "EPSG", "code": 3857},
                },
            },
        },
    }

    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta, b"geo": json.dumps(geo_meta).encode("utf-8")}
    table = table.replace_schema_metadata(new_meta)

    pq.write_table(table, output_path)
    return output_path
