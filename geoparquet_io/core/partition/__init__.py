"""
Partition subpackage for GeoParquet partitioning operations.

This package provides functions for partitioning GeoParquet files
by various spatial indexing schemes (H3, S2, A5, Quadkey, KDTree).
"""

from geoparquet_io.core.partition.common import (
    PartitionAnalysisError,
    calculate_partition_stats,
    partition_by_column,
    preview_partition,
)

__all__ = [
    "PartitionAnalysisError",
    "calculate_partition_stats",
    "partition_by_column",
    "preview_partition",
]
