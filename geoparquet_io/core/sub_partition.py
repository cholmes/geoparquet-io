"""Sub-partition functionality for processing directories of parquet files."""

from __future__ import annotations

import os
from pathlib import Path


def find_large_files(
    directory: str,
    min_size_bytes: int,
    recursive: bool = True,
) -> list[str]:
    """
    Find parquet files in a directory that exceed the size threshold.

    Args:
        directory: Directory to search
        min_size_bytes: Minimum file size in bytes
        recursive: Search subdirectories (default: True)

    Returns:
        List of file paths exceeding the threshold, sorted by size descending
    """
    large_files = []
    dir_path = Path(directory)

    pattern = "**/*.parquet" if recursive else "*.parquet"

    for parquet_file in dir_path.glob(pattern):
        if parquet_file.is_file():
            size = parquet_file.stat().st_size
            if size >= min_size_bytes:
                large_files.append((str(parquet_file), size))

    # Sort by size descending (largest first)
    large_files.sort(key=lambda x: x[1], reverse=True)

    return [f[0] for f in large_files]


def sub_partition_directory(
    directory: str,
    partition_type: str,
    min_size_bytes: int,
    resolution: int | None = None,
    level: int | None = None,
    in_place: bool = False,
    hive: bool = False,
    overwrite: bool = False,
    verbose: bool = False,
    force: bool = False,
    skip_analysis: bool = True,
    compression: str = "ZSTD",
    compression_level: int = 15,
    auto: bool = False,
    target_rows: int = 100000,
    max_partitions: int = 10000,
) -> dict:
    """
    Sub-partition large files in a directory.

    Finds all parquet files exceeding min_size_bytes and partitions them
    using the specified spatial index type.

    Args:
        directory: Directory containing parquet files
        partition_type: Type of partition ("h3", "s2", "quadkey")
        min_size_bytes: Minimum file size to process
        resolution: Resolution for H3/quadkey (0-15 for H3)
        level: Level for S2 (alias for resolution)
        in_place: If True, delete original after successful sub-partition
        hive: Use Hive-style partitioning
        overwrite: Overwrite existing output directories
        verbose: Print verbose output
        force: Force operation even with warnings
        skip_analysis: Skip partition analysis (default True for batch)
        compression: Compression codec
        compression_level: Compression level
        auto: Auto-calculate resolution
        target_rows: Target rows per partition for auto mode
        max_partitions: Max partitions for auto mode

    Returns:
        dict with keys: processed, skipped, errors
    """
    from geoparquet_io.core.logging_config import (
        configure_verbose,
        debug,
        info,
        progress,
        success,
        warn,
    )
    from geoparquet_io.core.partition.by_h3 import partition_by_h3
    from geoparquet_io.core.partition.by_quadkey import partition_by_quadkey
    from geoparquet_io.core.partition.by_s2 import partition_by_s2

    configure_verbose(verbose)

    # Map partition types to their functions and resolution param names
    partition_funcs = {
        "h3": (partition_by_h3, "resolution"),
        "s2": (partition_by_s2, "level"),
        "quadkey": (partition_by_quadkey, "resolution"),
    }

    if partition_type not in partition_funcs:
        raise ValueError(
            f"Unknown partition type: {partition_type}. "
            f"Must be one of: {list(partition_funcs.keys())}"
        )

    func, res_param = partition_funcs[partition_type]

    # Handle resolution/level parameter
    res_value = resolution if resolution is not None else level
    if not auto and res_value is None:
        raise ValueError(f"Must specify resolution/level or auto for {partition_type} partitioning")

    large_files = find_large_files(directory, min_size_bytes)

    if not large_files:
        info(f"No files found exceeding {min_size_bytes / (1024 * 1024):.1f}MB in {directory}")
        return {"processed": 0, "skipped": 0, "errors": []}

    progress(f"Found {len(large_files)} file(s) exceeding threshold")

    processed = 0
    skipped = 0
    errors = []

    for file_path in large_files:
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        file_name = os.path.basename(file_path)
        file_stem = Path(file_path).stem
        file_dir = os.path.dirname(file_path)

        # Output directory is sibling of input file
        output_dir = os.path.join(file_dir, f"{file_stem}_{partition_type}")

        progress(f"Processing: {file_name} ({file_size_mb:.1f}MB)")

        try:
            # Build kwargs for partition function
            kwargs = {
                "input_parquet": file_path,
                "output_folder": output_dir,
                "hive": hive,
                "overwrite": overwrite,
                "verbose": verbose,
                "force": force,
                "skip_analysis": skip_analysis,
                "compression": compression,
                "compression_level": compression_level,
                "auto": auto,
                "target_rows": target_rows,
                "max_partitions": max_partitions,
            }

            # Add resolution parameter with correct name
            if res_value is not None:
                kwargs[res_param] = res_value

            func(**kwargs)

            if in_place:
                # Validate output before deleting original
                output_files = list(Path(output_dir).glob("**/*.parquet"))
                if not output_files:
                    raise RuntimeError(
                        f"Sub-partition created no output files, keeping original: {file_path}"
                    )
                os.remove(file_path)
                debug(f"Removed original: {file_path}")

            processed += 1
            success(f"  Created: {output_dir}/")

        except Exception as e:
            warn(f"  ERROR: {e}")
            errors.append({"file": file_path, "error": str(e)})

    return {"processed": processed, "skipped": skipped, "errors": errors}
