# PR #364 Full Remediation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all issues identified in adversarial review of PR #364 - no shortcuts, complete implementation.

**Architecture:**
1. Create `core/exceptions.py` with framework-agnostic exceptions
2. Replace all 192 click exception instances across 31 files with core exceptions
3. Add CLI adapter to convert core exceptions to click exceptions at CLI boundary
4. Fix Phase 2 module duplication by removing duplicates from common.py and adding re-exports
5. Fix security issues (SQL injection, credential logging)
6. Add comprehensive unit tests

**Tech Stack:** Python 3.11+, Click, pytest

---

## Phase 0: Foundation - Create Core Exceptions Module

### Task 0.1: Create core/exceptions.py

**Files:**
- Create: `geoparquet_io/core/exceptions.py`
- Test: `tests/test_core_exceptions.py`

**Step 1: Write the test file**

```python
# tests/test_core_exceptions.py
"""Tests for core exception classes."""
import pytest
from geoparquet_io.core.exceptions import (
    GeoParquetError,
    FileNotFoundGeoParquetError,
    InvalidParameterError,
    RemoteAccessError,
    GeometryError,
    PartitionError,
    ValidationError,
)


class TestCoreExceptions:
    """Test that core exceptions are framework-agnostic."""

    def test_base_exception_inherits_from_exception(self):
        """GeoParquetError should inherit from Exception, not click exceptions."""
        assert issubclass(GeoParquetError, Exception)
        # Should NOT inherit from click
        import click
        assert not issubclass(GeoParquetError, click.ClickException)

    def test_file_not_found_error(self):
        with pytest.raises(FileNotFoundGeoParquetError) as exc_info:
            raise FileNotFoundGeoParquetError("test.parquet")
        assert "test.parquet" in str(exc_info.value)

    def test_invalid_parameter_error(self):
        with pytest.raises(InvalidParameterError) as exc_info:
            raise InvalidParameterError("resolution", "must be between 1-15")
        assert "resolution" in str(exc_info.value)
        assert "must be between 1-15" in str(exc_info.value)

    def test_remote_access_error(self):
        with pytest.raises(RemoteAccessError):
            raise RemoteAccessError("s3://bucket/file", "Access denied")

    def test_geometry_error(self):
        with pytest.raises(GeometryError):
            raise GeometryError("No geometry column found")

    def test_partition_error(self):
        with pytest.raises(PartitionError):
            raise PartitionError("Invalid partition scheme")

    def test_validation_error(self):
        with pytest.raises(ValidationError):
            raise ValidationError("File does not conform to GeoParquet spec")

    def test_exception_hierarchy(self):
        """All specific exceptions should inherit from GeoParquetError."""
        assert issubclass(FileNotFoundGeoParquetError, GeoParquetError)
        assert issubclass(InvalidParameterError, GeoParquetError)
        assert issubclass(RemoteAccessError, GeoParquetError)
        assert issubclass(GeometryError, GeoParquetError)
        assert issubclass(PartitionError, GeoParquetError)
        assert issubclass(ValidationError, GeoParquetError)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_core_exceptions.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'geoparquet_io.core.exceptions'"

**Step 3: Create the exceptions module**

```python
# geoparquet_io/core/exceptions.py
"""
Framework-agnostic exceptions for geoparquet-io core modules.

Core modules must NOT import click or raise click exceptions.
These exceptions are converted to click exceptions at the CLI boundary
by the cli/exception_handler.py module.

Exception Hierarchy:
    GeoParquetError (base)
    ├── FileNotFoundGeoParquetError - file/path not found
    ├── InvalidParameterError - invalid function argument
    ├── RemoteAccessError - S3/GCS/Azure access issues
    ├── GeometryError - geometry column issues
    ├── PartitionError - partitioning failures
    └── ValidationError - spec validation failures
"""

from __future__ import annotations


class GeoParquetError(Exception):
    """Base exception for all geoparquet-io errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class FileNotFoundGeoParquetError(GeoParquetError):
    """Raised when a required file or path is not found."""

    def __init__(self, path: str, detail: str | None = None) -> None:
        self.path = path
        msg = f"File not found: {path}"
        if detail:
            msg = f"{msg} - {detail}"
        super().__init__(msg)


class InvalidParameterError(GeoParquetError):
    """Raised when a function parameter has an invalid value."""

    def __init__(self, param_name: str, reason: str) -> None:
        self.param_name = param_name
        self.reason = reason
        super().__init__(f"Invalid parameter '{param_name}': {reason}")


class RemoteAccessError(GeoParquetError):
    """Raised when remote file access (S3/GCS/Azure) fails."""

    def __init__(self, url: str, reason: str) -> None:
        # Sanitize URL to avoid logging credentials
        self.url = self._sanitize_url(url)
        self.reason = reason
        super().__init__(f"Remote access error for {self.url}: {reason}")

    @staticmethod
    def _sanitize_url(url: str) -> str:
        """Remove credentials and query params from URL for safe logging."""
        # Remove query string (may contain presigned credentials)
        if "?" in url:
            url = url.split("?")[0]
        # Truncate path for readability
        parts = url.split("/")
        if len(parts) > 4:
            return "/".join(parts[:4]) + "/..."
        return url


class GeometryError(GeoParquetError):
    """Raised when geometry column operations fail."""

    pass


class PartitionError(GeoParquetError):
    """Raised when partitioning operations fail."""

    pass


class ValidationError(GeoParquetError):
    """Raised when GeoParquet validation fails."""

    pass
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_core_exceptions.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/exceptions.py tests/test_core_exceptions.py
git commit -m "feat(core): add framework-agnostic exception classes"
```

---

### Task 0.2: Create CLI Exception Handler

**Files:**
- Create: `geoparquet_io/cli/exception_handler.py`
- Test: `tests/cli/test_exception_handler.py`

**Step 1: Write the test file**

```python
# tests/cli/test_exception_handler.py
"""Tests for CLI exception handler."""
import pytest
import click
from geoparquet_io.cli.exception_handler import handle_core_exception, core_exception_handler
from geoparquet_io.core.exceptions import (
    GeoParquetError,
    FileNotFoundGeoParquetError,
    InvalidParameterError,
    RemoteAccessError,
)


class TestExceptionHandler:
    """Test CLI exception handling."""

    def test_converts_file_not_found_to_click_exception(self):
        exc = FileNotFoundGeoParquetError("test.parquet")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "test.parquet" in click_exc.message

    def test_converts_invalid_parameter_to_bad_parameter(self):
        exc = InvalidParameterError("resolution", "must be 1-15")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.BadParameter)
        assert "resolution" in str(click_exc.message)

    def test_converts_remote_access_error_to_click_exception(self):
        exc = RemoteAccessError("s3://bucket/file", "Access denied")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)

    def test_decorator_catches_and_converts_exceptions(self):
        @core_exception_handler
        def failing_function():
            raise FileNotFoundGeoParquetError("missing.parquet")

        with pytest.raises(click.ClickException) as exc_info:
            failing_function()
        assert "missing.parquet" in str(exc_info.value)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/cli/test_exception_handler.py -v`
Expected: FAIL

**Step 3: Create the exception handler**

```python
# geoparquet_io/cli/exception_handler.py
"""
Convert core exceptions to click exceptions at CLI boundary.

This module bridges the gap between framework-agnostic core exceptions
and click-specific exceptions needed for proper CLI error display.
"""
from __future__ import annotations

import functools
from typing import Callable, TypeVar

import click

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
)

F = TypeVar("F", bound=Callable)


def handle_core_exception(exc: GeoParquetError) -> click.ClickException:
    """Convert a core exception to the appropriate click exception."""
    if isinstance(exc, InvalidParameterError):
        return click.BadParameter(exc.message, param_hint=exc.param_name)
    elif isinstance(exc, FileNotFoundGeoParquetError):
        return click.ClickException(exc.message)
    elif isinstance(exc, RemoteAccessError):
        return click.ClickException(exc.message)
    elif isinstance(exc, PartitionError):
        return click.ClickException(exc.message)
    elif isinstance(exc, ValidationError):
        return click.ClickException(exc.message)
    else:
        # Generic fallback for any GeoParquetError subclass
        return click.ClickException(exc.message)


def core_exception_handler(func: F) -> F:
    """
    Decorator to catch core exceptions and convert to click exceptions.

    Use this decorator on CLI command functions to automatically convert
    framework-agnostic core exceptions to click exceptions.

    Example:
        @click.command()
        @core_exception_handler
        def my_command():
            # If this raises InvalidParameterError, it becomes click.BadParameter
            do_something_that_might_fail()
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except GeoParquetError as e:
            raise handle_core_exception(e)
    return wrapper  # type: ignore
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/cli/test_exception_handler.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/cli/exception_handler.py tests/cli/test_exception_handler.py
git commit -m "feat(cli): add exception handler to convert core exceptions to click"
```

---

## Phase 1: Fix Import-Linter Config

### Task 1.1: Update pyproject.toml

**Files:**
- Modify: `pyproject.toml:307-310`

**Step 1: Update ignore_imports to include subpackages**

```python
ignore_imports = [
    "geoparquet_io.core.* -> click",
    "geoparquet_io.core.partition.* -> click",
    "geoparquet_io.core.add.* -> click",
    "geoparquet_io.core.benchmark_suite -> geoparquet_io",
]
```

**Step 2: Verify import-linter passes**

Run: `uv run lint-imports`
Expected: No contract violations (with ignores in place)

**Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "fix(config): update import-linter to ignore subpackage click imports"
```

---

## Phase 2: Fix Phase 2 Module Duplication

### Task 2.1: Fix duckdb_utils.py duplication

**Files:**
- Modify: `geoparquet_io/core/common.py`
- Modify: `geoparquet_io/core/duckdb_utils.py`

**Step 1: Identify what to remove from common.py**

Functions duplicated in both files:
- `_s3_buckets_needing_auth` (cache set)
- `_extract_bucket_name()`
- `_needs_s3_auth()`
- `_clear_s3_cache()`
- `quote_identifier()`
- `get_duckdb_connection()`
- `get_duckdb_connection_for_s3()`

**Step 2: Remove duplicates from common.py and add re-exports**

In common.py, REMOVE lines 46-413 (the duplicated functions) and REPLACE with:

```python
# Re-exports from duckdb_utils (Phase 2 extraction)
from geoparquet_io.core.duckdb_utils import (
    _clear_s3_cache,
    _extract_bucket_name,
    _needs_s3_auth,
    _s3_buckets_needing_auth,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    quote_identifier,
)
```

**Step 3: Ensure duckdb_utils.py exports these at module level**

Verify `__all__` in duckdb_utils.py includes all exported symbols.

**Step 4: Run tests**

Run: `uv run pytest tests/test_common_utils.py -v`
Expected: PASS (re-exports work)

**Step 5: Commit**

```bash
git add geoparquet_io/core/common.py geoparquet_io/core/duckdb_utils.py
git commit -m "refactor(core): remove duckdb_utils duplication from common.py, add re-exports"
```

---

### Task 2.2: Fix crs_utils.py duplication

**Files:**
- Modify: `geoparquet_io/core/common.py`

**Step 1: Identify CRS functions to remove from common.py**

Functions that exist in crs_utils.py:
- `_extract_crs_identifier()`
- CRS-related utilities

**Step 2: Remove from common.py and add re-exports**

```python
from geoparquet_io.core.crs_utils import (
    _extract_crs_identifier,
    # ... other CRS functions
)
```

**Step 3: Run tests**

Run: `uv run pytest -k crs -v`
Expected: PASS

**Step 4: Commit**

```bash
git add geoparquet_io/core/common.py
git commit -m "refactor(core): remove crs_utils duplication from common.py, add re-exports"
```

---

### Task 2.3: Fix geo_metadata.py duplication

**Files:**
- Modify: `geoparquet_io/core/common.py`

**Step 1: Identify metadata functions to remove from common.py**

Functions that exist in geo_metadata.py:
- `parse_geo_metadata()`
- `create_geo_metadata()`
- `check_bbox_structure()`
- etc.

**Step 2: Remove from common.py and add re-exports**

**Step 3: Run tests**

Run: `uv run pytest -k metadata -v`
Expected: PASS

**Step 4: Commit**

```bash
git add geoparquet_io/core/common.py
git commit -m "refactor(core): remove geo_metadata duplication from common.py, add re-exports"
```

---

### Task 2.4: Fix parquet_writer.py duplication

**Files:**
- Modify: `geoparquet_io/core/common.py`
- Modify: `geoparquet_io/core/parquet_writer.py`

**Step 1: Fix API mismatch**

`validate_compression_settings()` returns 2 values in parquet_writer.py but 3 in common.py.
Standardize on the 3-value return (with codec-specific validation).

**Step 2: Remove from common.py and add re-exports**

```python
from geoparquet_io.core.parquet_writer import (
    ParquetWriteSettings,
    validate_compression_settings,
    # ... other writer functions
)
```

**Step 3: Run tests**

Run: `uv run pytest -k parquet -v`
Expected: PASS

**Step 4: Commit**

```bash
git add geoparquet_io/core/common.py geoparquet_io/core/parquet_writer.py
git commit -m "refactor(core): remove parquet_writer duplication from common.py, add re-exports"
```

---

## Phase 3: Replace Click Exceptions in Core Modules

### Task 3.1: Replace click exceptions in file_utils.py

**Files:**
- Modify: `geoparquet_io/core/file_utils.py`

**Step 1: Replace imports**

Remove: `import click`
Add:
```python
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    InvalidParameterError,
)
```

**Step 2: Replace all click.ClickException with FileNotFoundGeoParquetError**

Line 126, 128, 146, 167, 174: Replace `raise click.ClickException(...)` with `raise FileNotFoundGeoParquetError(...)`

**Step 3: Replace click.BadParameter with InvalidParameterError**

Line 197: Replace `raise click.BadParameter(...)` with `raise InvalidParameterError(...)`

**Step 4: Run tests**

Run: `uv run pytest tests/ -k "file" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/file_utils.py
git commit -m "refactor(core): replace click exceptions with core exceptions in file_utils"
```

---

### Task 3.2: Replace click exceptions in remote.py

**Files:**
- Modify: `geoparquet_io/core/remote.py`

**Step 1: Replace imports**

Remove: `import click`
Add:
```python
from geoparquet_io.core.exceptions import (
    InvalidParameterError,
    RemoteAccessError,
)
```

**Step 2: Replace click.BadParameter**

Line 168: Replace with `InvalidParameterError`

**Step 3: Sanitize URL logging**

Lines 185-189, 266-278, 317-318: Use `RemoteAccessError._sanitize_url()` before logging URLs

**Step 4: Run tests**

Run: `uv run pytest tests/ -k "remote" -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/remote.py
git commit -m "refactor(core): replace click exceptions and sanitize URLs in remote.py"
```

---

### Task 3.3-3.31: Replace click exceptions in remaining 29 files

For EACH of these files, follow the same pattern:

**Files with click imports (31 total):**
1. `geoparquet_io/core/format_writers.py`
2. `geoparquet_io/core/common.py`
3. `geoparquet_io/core/stac_check.py`
4. `geoparquet_io/core/extract.py`
5. `geoparquet_io/core/hilbert_order.py`
6. `geoparquet_io/core/add/quadkey.py`
7. `geoparquet_io/core/add/h3.py`
8. `geoparquet_io/core/add/a5.py`
9. `geoparquet_io/core/add/s2.py`
10. `geoparquet_io/core/add/bbox_metadata.py`
11. `geoparquet_io/core/add/country_codes.py`
12. `geoparquet_io/core/add/kdtree.py`
13. `geoparquet_io/core/benchmark.py`
14. `geoparquet_io/core/convert.py`
15. `geoparquet_io/core/stac.py`
16. `geoparquet_io/core/sort_by_column.py`
17. `geoparquet_io/core/sort_quadkey.py`
18. `geoparquet_io/core/admin_datasets.py`
19. `geoparquet_io/core/partition/common.py`
20. `geoparquet_io/core/partition/reader.py`
21. `geoparquet_io/core/partition/by_quadkey.py`
22. `geoparquet_io/core/partition/by_s2.py`
23. `geoparquet_io/core/partition/by_kdtree.py`
24. `geoparquet_io/core/partition/by_string.py`
25. `geoparquet_io/core/partition/by_a5.py`
26. `geoparquet_io/core/partition/admin_hierarchical.py`
27. `geoparquet_io/core/partition/by_h3.py`
28. `geoparquet_io/core/arcgis.py`
29. `geoparquet_io/core/check_fixes.py`

**For each file:**

1. Remove `import click`
2. Add appropriate core exception imports
3. Replace each `click.ClickException` → `GeoParquetError` subclass
4. Replace each `click.BadParameter` → `InvalidParameterError`
5. Replace each `click.UsageError` → `InvalidParameterError` or `GeoParquetError`
6. Run relevant tests
7. Commit with message: `refactor(core): replace click exceptions in <module_name>`

---

## Phase 4: Fix Security Issues

### Task 4.1: Fix SQL injection in duckdb_utils.py

**Files:**
- Modify: `geoparquet_io/core/duckdb_utils.py`
- Test: `tests/test_duckdb_utils.py`

**Step 1: Write test for SQL injection**

```python
# In tests/test_duckdb_utils.py
def test_path_with_single_quote_is_escaped():
    """Paths with single quotes should not break SQL."""
    from geoparquet_io.core.duckdb_utils import _escape_sql_string

    dangerous_path = "s3://bucket/test'file.parquet"
    escaped = _escape_sql_string(dangerous_path)
    assert "'" not in escaped or "''" in escaped
```

**Step 2: Add _escape_sql_string helper**

```python
def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL string literals."""
    return value.replace("'", "''")
```

**Step 3: Use escaping in all SQL string interpolation**

**Step 4: Run tests**

Run: `uv run pytest tests/test_duckdb_utils.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add geoparquet_io/core/duckdb_utils.py tests/test_duckdb_utils.py
git commit -m "fix(security): escape single quotes in SQL to prevent injection"
```

---

### Task 4.2: Fix SQL injection in file_utils.py

**Files:**
- Modify: `geoparquet_io/core/file_utils.py`

**Step 1: Update safe_file_url() to escape single quotes**

```python
def safe_file_url(path: str) -> str:
    """Convert path to safe file URL for DuckDB SQL.

    Escapes single quotes to prevent SQL injection.
    """
    # ... existing logic ...
    # Add escaping
    result = result.replace("'", "''")
    return result
```

**Step 2: Run tests**

Run: `uv run pytest tests/ -k file_utils -v`
Expected: PASS

**Step 3: Commit**

```bash
git add geoparquet_io/core/file_utils.py
git commit -m "fix(security): escape single quotes in safe_file_url"
```

---

## Phase 5: Fix Broken Test

### Task 5.1: Update test_sub_partition.py monkeypatch path

**Files:**
- Modify: `tests/test_sub_partition.py:186-188`

**Step 1: Update the monkeypatch path**

```python
# OLD (broken)
monkeypatch.setattr(
    "geoparquet_io.core.partition_by_h3.partition_by_h3", mock_partition_fail
)

# NEW (correct)
monkeypatch.setattr(
    "geoparquet_io.core.partition.by_h3.partition_by_h3", mock_partition_fail
)
```

**Step 2: Run the test**

Run: `uv run pytest tests/test_sub_partition.py -v`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/test_sub_partition.py
git commit -m "fix(tests): update monkeypatch path for renamed partition module"
```

---

## Phase 6: Add Unit Tests for New Modules

### Task 6.1: Create tests/test_file_utils.py

**Files:**
- Create: `tests/test_file_utils.py`

**Test coverage for:**
- `get_first_parquet_file()`
- `is_partition_path()`
- `safe_file_url()`
- `_get_file_cache_key()`

### Task 6.2: Create tests/test_geometry_detection.py

**Files:**
- Create: `tests/test_geometry_detection.py`

**Test coverage for:**
- `STANDARD_GEOMETRY_NAMES`
- `find_primary_geometry_column()`
- `_detect_geometry_from_query()`

### Task 6.3: Create tests/test_duckdb_utils.py

**Files:**
- Create: `tests/test_duckdb_utils.py`

**Test coverage for:**
- `get_duckdb_connection()`
- `get_duckdb_connection_for_s3()`
- `quote_identifier()`
- `_escape_sql_string()`

### Task 6.4: Create tests/test_parquet_writer.py

**Files:**
- Create: `tests/test_parquet_writer.py`

**Test coverage for:**
- `ParquetWriteSettings`
- `validate_compression_settings()`

### Task 6.5: Create tests/test_crs_utils.py

**Files:**
- Create: `tests/test_crs_utils.py`

### Task 6.6: Create tests/test_geo_metadata.py

**Files:**
- Create: `tests/test_geo_metadata.py`

---

## Phase 7: Final Verification

### Task 7.1: Run full test suite

Run: `uv run pytest -n auto -v`
Expected: All tests pass, coverage >= 67%

### Task 7.2: Run import-linter

Run: `uv run lint-imports`
Expected: No contract violations

### Task 7.3: Verify no click imports remain in core

Run: `grep -rn "import click" geoparquet_io/core/ --include="*.py" | grep -v "__pycache__"`
Expected: No output (all click imports removed)

### Task 7.4: Run ruff

Run: `uv run ruff check geoparquet_io/`
Expected: No errors

### Task 7.5: Final commit

```bash
git add -A
git commit -m "chore: final cleanup after PR #364 remediation"
```

---

## Summary Checklist

- [ ] Phase 0: Create core/exceptions.py and cli/exception_handler.py
- [ ] Phase 1: Fix import-linter config
- [ ] Phase 2: Fix Phase 2 module duplication (4 tasks)
- [ ] Phase 3: Replace click exceptions in 31 files (192 instances)
- [ ] Phase 4: Fix SQL injection (2 locations)
- [ ] Phase 5: Fix broken test
- [ ] Phase 6: Add unit tests (6 new test files)
- [ ] Phase 7: Final verification

**Estimated time:** 4-6 hours of focused work
**Estimated commits:** 40-50 commits
