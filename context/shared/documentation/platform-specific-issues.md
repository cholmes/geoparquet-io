# Platform-Specific Issues

This document catalogs known platform-specific issues and their solutions. These are recurring problems that may surface again in similar contexts.

## Windows File Locking with PyArrow

### Problem

On Windows, when `pq.ParquetFile(path)` throws an exception (e.g., invalid Parquet file), the file handle may not be released immediately. Python's garbage collector doesn't clean up the `ParquetFile` object right away, which keeps the file locked.

This causes `PermissionError: [WinError 32]` when attempting to delete the file:

```
PermissionError: [WinError 32] The process cannot access the file because it
is being used by another process: 'C:\...\tmp123.parquet'
```

### Solution

Always use a `finally` block to explicitly delete the `ParquetFile` object:

```python
def read_parquet_metadata(parquet_file: str) -> dict:
    pf = None  # Initialize to None
    try:
        pf = pq.ParquetFile(parquet_file)
        return dict(pf.metadata.metadata) if pf.metadata.metadata else {}
    except Exception as e:
        raise SomeError(f"Cannot read: {parquet_file}") from e
    finally:
        # Explicitly delete to release file handle (Windows compatibility)
        del pf
```

### Files Affected

- `geoparquet_io/core/duckdb_metadata.py`:
  - `_pyarrow_get_kv_metadata()`
  - `_pyarrow_get_geo_metadata()`
  - `_pyarrow_get_schema_info()`

### References

- PR #233 (inspect metadata performance)
- Windows CI failures in `test_duckdb_metadata.py::TestGeoParquetErrorExceptions`

---

## DuckDB Connection Cleanup on Windows

### Problem

On Windows, DuckDB connections must be explicitly closed before temporary files can be deleted. Unlike Unix where files can be deleted while open, Windows enforces strict file locking.

### Solution

Always use try/finally or context managers for DuckDB connections:

```python
con = duckdb.connect()
try:
    # ... use connection
finally:
    con.close()
```

Or use UUID in temporary filenames to avoid collisions:

```python
temp_file = f"/tmp/geoparquet_{uuid.uuid4().hex}.parquet"
```

### References

- Multiple tests throughout the codebase
- CLAUDE.md mentions this under "Debugging"

---

## macOS ARM64 SIGABRT with Sequential GeoPackage Layer Reads

### Problem

On macOS ARM64 (Apple Silicon), reading multiple layers from a GeoPackage file sequentially can cause sporadic SIGABRT crashes. The crash typically occurs on the 2nd or 3rd layer read, not the first.

This is caused by a race condition between DuckDB connection cleanup and GDAL's internal state. DuckDB's spatial extension uses GDAL for `ST_Read()`, and GDAL maintains internal global state for SQLite/GeoPackage connections. When a DuckDB connection is closed, GDAL's cleanup may not complete before a new connection opens.

The issue is specific to:
- macOS (Darwin)
- ARM64 architecture (Apple Silicon)
- Sequential reads from multi-layer files (GeoPackage, FileGDB)

### Symptoms

```
Fatal Python error: Aborted

Thread 0x000000016f747000 (most recent call first):
  File ".../geoparquet_io/core/convert.py", line 1033 in read_spatial_to_arrow
  ...
```

### Solution

Force garbage collection after closing DuckDB connections when reading spatial files on macOS ARM64:

```python
import gc
import platform

con = get_duckdb_connection(load_spatial=True)
try:
    # ... use ST_Read() to read from GeoPackage
finally:
    con.close()
    # Force GC on macOS ARM64 to prevent SIGABRT with sequential reads
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        gc.collect()
```

### Files Affected

- `geoparquet_io/core/convert.py`:
  - `read_spatial_to_arrow()` - Fixed with gc.collect() after con.close()

### Workaround for User Code

If using the Python API to convert multiple layers, add explicit garbage collection:

```python
import gc
import geoparquet_io as gpio

for layer in layers:
    gpio.convert(gpkg_path, layer=layer).write(f"{layer}.parquet")
    gc.collect()  # Prevent SIGABRT on macOS ARM64
```

### References

- Issue #322 (SIGABRT in read_spatial_to_arrow when converting multiple GeoPackage layers)
- Related: DuckDB spatial extension uses GDAL internally for vector format support
