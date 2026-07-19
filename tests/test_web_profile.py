from geoparquet_io.core.parquet_writer import ParquetWriteSettings
from geoparquet_io.core.web_profile import (
    WEB_GEOPARQUET_VERSION,
    WEB_ROW_GROUP_ROWS_MAX,
    WEB_ROW_GROUP_ROWS_MIN,
    WEB_WRITE_STRATEGY,
    resolve_web_row_group_rows,
    resolve_web_settings,
)


def test_web_profile_settings_defaults():
    s = resolve_web_settings()
    assert isinstance(s, ParquetWriteSettings)
    assert s.write_page_index is True
    assert s.data_page_size is None  # pyarrow default page size for the web profile
    assert s.compression == "ZSTD"


def test_web_profile_constants_stable():
    assert WEB_GEOPARQUET_VERSION == "2.0"
    assert WEB_WRITE_STRATEGY == "streaming"


def test_row_group_equation_targets_bytes():
    # 1,000,000 rows, 200 MB input -> 200 bytes/row. 8 MiB target -> ~41,943 rows.
    rows = resolve_web_row_group_rows(1_000_000, 200 * 1024 * 1024, target_mb=8)
    assert 30_000 <= rows <= 60_000


def test_row_group_equation_clamps_min_for_huge_features():
    # 1,000 rows, 2 GB input -> 2 MB/row. Naive target would give < 10 rows; clamp to MIN.
    rows = resolve_web_row_group_rows(1_000, 2 * 1024 * 1024 * 1024, target_mb=8)
    assert rows == min(WEB_ROW_GROUP_ROWS_MIN, 1_000)  # capped by total rows too


def test_row_group_equation_clamps_max_for_tiny_features():
    # 100M rows, tiny points -> naive target would give millions of rows; clamp to MAX.
    rows = resolve_web_row_group_rows(100_000_000, 100 * 1024 * 1024, target_mb=8)
    assert rows <= WEB_ROW_GROUP_ROWS_MAX


def test_row_group_equation_footer_guard():
    # Very large row count with small clamp would create too many groups; guard raises rows.
    rows = resolve_web_row_group_rows(500_000_000, 5 * 1024 * 1024 * 1024, target_mb=8)
    assert 500_000_000 / rows <= 1000


def test_explicit_rows_override_wins():
    assert resolve_web_row_group_rows(1_000_000, 200 * 1024 * 1024, explicit_rows=25_000) == 25_000


def test_classmethod_matches_resolver():
    assert ParquetWriteSettings.for_web_profile().get_pyarrow_kwargs() == (
        resolve_web_settings().get_pyarrow_kwargs()
    )
