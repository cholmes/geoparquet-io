"""Tests that the six former bare-`duckdb.connect()` call sites now route
through `get_duckdb_connection()` (core/duckdb_utils.py) and therefore pick up
its mandatory session settings:

- ``arrow_large_buffer_size = true`` (without it, >2GB string/WKB Arrow
  exports fail)
- ``geometry_always_xy = true`` (DuckDB 1.5 axis-order correctness, when the
  spatial extension is loaded)

Each test patches the module's ``get_duckdb_connection`` binding with a spy
that delegates to the real factory, captures the settings on the *actual*
connection the call site ends up using, and lets the call proceed normally.
This proves both that the call site no longer bare-connects (the spy must be
invoked) and that the mandatory settings are functionally present on the
resulting connection.

Also covers the pre-commit `duckdb-antipatterns` hook extension that bans
bare `duckdb.connect(` outside `core/duckdb_utils.py`.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

from geoparquet_io.core.duckdb_utils import get_duckdb_connection as real_get_duckdb_connection

TEST_DATA_DIR = Path(__file__).parent / "data"
GEOJSON_FILE = TEST_DATA_DIR / "buildings_test.geojson"

REPO_ROOT = Path(__file__).parent.parent


def _spy_factory(monkeypatch, module, attr="get_duckdb_connection"):
    """Patch ``module.<attr>`` with a spy that delegates to the real factory.

    Returns a list that accumulates one dict per call, each recording the
    kwargs passed and the mandatory settings observed on the resulting
    connection (captured immediately, before the caller can close it).
    """
    captured = []

    def spy(*args, **kwargs):
        con = real_get_duckdb_connection(*args, **kwargs)
        record = {
            "kwargs": kwargs,
            "arrow_large_buffer_size": con.execute(
                "SELECT current_setting('arrow_large_buffer_size')"
            ).fetchone()[0],
        }
        if kwargs.get("load_spatial", True):
            record["geometry_always_xy"] = con.execute(
                "SELECT current_setting('geometry_always_xy')"
            ).fetchone()[0]
        captured.append(record)
        return con

    monkeypatch.setattr(module, attr, spy)
    return captured


class TestBenchmarkGetFileInfo:
    """geoparquet_io/core/benchmark.py:113 (get_file_info)."""

    def test_routes_through_factory_with_mandatory_settings(self, monkeypatch):
        import geoparquet_io.core.benchmark as benchmark

        captured = _spy_factory(monkeypatch, benchmark)

        info = benchmark.get_file_info(GEOJSON_FILE)

        assert "error" not in info, info
        assert len(captured) == 1
        assert captured[0]["arrow_large_buffer_size"] is True
        assert captured[0]["geometry_always_xy"] is True


class TestBenchmarkDuckdb:
    """geoparquet_io/core/benchmark.py:252 (benchmark_duckdb)."""

    def test_routes_through_factory_with_mandatory_settings(self, monkeypatch, tmp_path):
        import geoparquet_io.core.benchmark as benchmark

        captured = _spy_factory(monkeypatch, benchmark)
        output_path = tmp_path / "out.parquet"

        benchmark.benchmark_duckdb(GEOJSON_FILE, output_path)

        assert output_path.exists()
        assert len(captured) == 1
        assert captured[0]["arrow_large_buffer_size"] is True
        assert captured[0]["geometry_always_xy"] is True


class TestWkbToWktPreview:
    """geoparquet_io/core/inspect_utils.py:452 (wkb_to_wkt_preview)."""

    def test_routes_through_factory_with_mandatory_settings(self, monkeypatch):
        import geoparquet_io.core.inspect_utils as inspect_utils

        captured = _spy_factory(monkeypatch, inspect_utils)

        # Standard ISO WKB point (x=1, y=2), matches the byte-order-0x01 path.
        wkb_point = (
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
        )
        wkt = inspect_utils.wkb_to_wkt_preview(wkb_point)

        assert "POINT" in wkt
        assert len(captured) == 1
        assert captured[0]["arrow_large_buffer_size"] is True
        assert captured[0]["geometry_always_xy"] is True


class TestGetColumnStatistics:
    """geoparquet_io/core/inspect_utils.py:753 (get_column_statistics).

    Called out in project memory as the known bypass that also OMITS
    arrow_large_buffer_size inline (unlike the other bare-connect sites,
    which at least reimplemented geometry_always_xy).
    """

    def test_routes_through_factory_with_mandatory_settings(self, monkeypatch, fields_v2_file):
        import geoparquet_io.core.inspect_utils as inspect_utils

        captured = _spy_factory(monkeypatch, inspect_utils)

        columns_info = [{"name": "id", "is_geometry": False}]
        stats = inspect_utils.get_column_statistics(fields_v2_file, columns_info)

        assert "id" in stats
        assert len(captured) == 1
        assert captured[0]["arrow_large_buffer_size"] is True
        assert captured[0]["geometry_always_xy"] is True


class TestCountryCodesCreateConnection:
    """geoparquet_io/core/add/country_codes.py:515 (_create_duckdb_connection)."""

    def test_routes_through_factory_with_mandatory_settings(self, monkeypatch):
        import geoparquet_io.core.add.country_codes as country_codes

        captured = _spy_factory(monkeypatch, country_codes)

        con = country_codes._create_duckdb_connection(using_default=False)
        try:
            assert len(captured) == 1
            assert captured[0]["arrow_large_buffer_size"] is True
            assert captured[0]["geometry_always_xy"] is True
        finally:
            con.close()

    def test_using_default_still_sets_s3_region(self, monkeypatch):
        """The deliberate s3_region='us-west-2' for the default Overture source
        must be preserved through the factory, not dropped."""
        import geoparquet_io.core.add.country_codes as country_codes

        captured = _spy_factory(monkeypatch, country_codes)

        con = country_codes._create_duckdb_connection(using_default=True)
        try:
            assert captured[0]["kwargs"].get("s3_region") == "us-west-2"
            region = con.execute("SELECT current_setting('s3_region')").fetchone()[0]
            assert region == "us-west-2"
        finally:
            con.close()


class TestDiskRewriteWriteFromTable:
    """geoparquet_io/core/write_strategies/disk_rewrite.py:198 (write_from_table)."""

    def test_routes_through_factory_with_mandatory_settings(self, monkeypatch, tmp_path):
        import geoparquet_io.core.write_strategies.disk_rewrite as disk_rewrite

        captured = _spy_factory(monkeypatch, disk_rewrite)

        wkb_point = (
            b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
        )
        table = pa.table(
            {
                "id": [1, 2, 3],
                "geometry": [wkb_point, wkb_point, wkb_point],
            }
        )
        strategy = disk_rewrite.DiskRewriteStrategy()
        output_path = str(tmp_path / "out.parquet")

        strategy.write_from_table(
            table=table,
            output_path=output_path,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert Path(output_path).exists()
        assert len(captured) == 1
        assert captured[0]["arrow_large_buffer_size"] is True
        assert captured[0]["geometry_always_xy"] is True


class TestAntipatternHookBansBareConnect:
    """Self-test that the extended duckdb-antipatterns hook rejects a bare
    ``duckdb.connect(`` reintroduced outside core/duckdb_utils.py."""

    @staticmethod
    def _hook_script():
        """Extract the duckdb-antipatterns hook script from
        .pre-commit-config.yaml, exactly as pre-commit would invoke it."""
        import yaml

        config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text())
        for repo in config["repos"]:
            for hook in repo.get("hooks", []):
                if hook["id"] == "duckdb-antipatterns":
                    return hook["args"][-1]
        raise AssertionError("duckdb-antipatterns hook not found in .pre-commit-config.yaml")

    def _run_hook(self, cwd):
        return subprocess.run(
            ["bash", "-c", self._hook_script()],
            cwd=cwd,
            capture_output=True,
            text=True,
        )

    def test_hook_passes_on_clean_tree(self):
        """Sanity check: after the fix, the hook is green on the real,
        unmodified repo tree (all six routed sites plus the untouched
        factory internal connect)."""
        result = self._run_hook(cwd=REPO_ROOT)
        assert result.returncode == 0, result.stdout + result.stderr

    def _write_workspace(self, tmp_path, extra_files):
        """Build an isolated `geoparquet_io/` tree under tmp_path so these
        tests never mutate the real, shared repo tree — this test suite runs
        under pytest-xdist with parallel workers, and other tests in this
        file run the same hook against the real tree concurrently.
        """
        core_dir = tmp_path / "geoparquet_io" / "core"
        core_dir.mkdir(parents=True)
        # A minimal stand-in for the factory's own internal connect, which
        # must stay exempt.
        (core_dir / "duckdb_utils.py").write_text(
            "def get_duckdb_connection(config=None):\n"
            "    con = duckdb.connect(config=config) if config else duckdb.connect()\n"
            "    return con\n"
        )
        for name, content in extra_files.items():
            (core_dir / name).write_text(content)
        return tmp_path

    def test_hook_rejects_reintroduced_bare_connect(self, tmp_path):
        """A bare `duckdb.connect(` in a core module other than
        duckdb_utils.py must fail the hook."""
        workspace = self._write_workspace(
            tmp_path,
            {"some_module.py": "import duckdb\n\ncon = duckdb.connect()\n"},
        )
        result = self._run_hook(cwd=workspace)
        assert result.returncode != 0
        assert "duckdb.connect" in (result.stdout + result.stderr)

    def test_hook_ignores_the_factorys_own_internal_connect(self, tmp_path):
        """duckdb_utils.py's own `duckdb.connect(...)` must stay allowed —
        it's the factory's internal implementation, not a bypass."""
        workspace = self._write_workspace(tmp_path, {})
        result = self._run_hook(cwd=workspace)
        assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
