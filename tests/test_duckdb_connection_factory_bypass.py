"""Tests that the six former bare-`duckdb.connect()` call sites now route
through `get_duckdb_connection()` (core/duckdb_utils.py).

Two separable claims are tested separately:

1. The factory applies the mandatory session settings
   (``arrow_large_buffer_size`` for >2GB string/WKB Arrow exports and, when the
   spatial extension is loaded, ``geometry_always_xy`` for DuckDB 1.5 axis-order
   correctness). That is the factory's own contract, so it is asserted exactly
   once, in ``TestFactoryAppliesMandatorySettings``.

2. Each call site delegates to the factory, and asks it for the right thing.
   Those tests patch the module's ``get_duckdb_connection`` binding with a spy
   that records the kwargs and delegates to the real factory. Re-checking the
   settings there would prove nothing about the call site, so instead each one
   pins the kwargs *it* chose — which is what a call site can actually get
   wrong (e.g. skipping httpfs for a remote file, or disabling spatial).

Also covers the pre-commit `duckdb-antipatterns` hook extension that bans
bare `duckdb.connect(` outside `core/duckdb_utils.py`.
"""

import subprocess
import time
from pathlib import Path

import pyarrow as pa
import pytest
import yaml

from geoparquet_io.core.duckdb_utils import get_duckdb_connection as real_get_duckdb_connection

TEST_DATA_DIR = Path(__file__).parent / "data"
GEOJSON_FILE = TEST_DATA_DIR / "buildings_test.geojson"

REPO_ROOT = Path(__file__).parent.parent

# Standard ISO WKB point (x=1, y=2), matches the byte-order-0x01 path.
WKB_POINT = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"


def _spy_factory(monkeypatch, module) -> list[dict]:
    """Patch ``module.get_duckdb_connection`` with a delegating spy.

    Returns a list that accumulates the kwargs of each call, so a test can
    assert both that the call site went through the factory (the spy ran) and
    what it asked the factory for.
    """
    captured: list[dict] = []

    def spy(*args, **kwargs):
        assert not args, f"call site should pass keyword arguments, got {args!r}"
        captured.append(kwargs)
        return real_get_duckdb_connection(**kwargs)

    monkeypatch.setattr(module, "get_duckdb_connection", spy)
    return captured


def _kwargs_only_spy(monkeypatch, module) -> list[dict]:
    """Like ``_spy_factory``, but never actually loads httpfs.

    For tests that only care which extensions a call site *requests* — honoring
    load_httpfs=True would make DuckDB install the extension over the network.
    """
    captured: list[dict] = []

    def spy(**kwargs):
        captured.append(kwargs)
        return real_get_duckdb_connection(**{**kwargs, "load_httpfs": False})

    monkeypatch.setattr(module, "get_duckdb_connection", spy)
    return captured


class TestFactoryAppliesMandatorySettings:
    """core/duckdb_utils.py (get_duckdb_connection).

    The single place these settings are asserted. The call-site tests below
    rely on this contract rather than re-proving it six times.
    """

    def test_arrow_large_buffer_size_is_always_set(self):
        with real_get_duckdb_connection(load_spatial=False, load_httpfs=False) as con:
            setting = con.execute("SELECT current_setting('arrow_large_buffer_size')").fetchone()
        assert setting[0] is True

    def test_geometry_always_xy_is_set_when_spatial_is_loaded(self):
        with real_get_duckdb_connection(load_httpfs=False) as con:
            setting = con.execute("SELECT current_setting('geometry_always_xy')").fetchone()
        assert setting[0] is True


class TestBenchmarkGetFileInfo:
    """core/benchmark.py (get_file_info)."""

    def test_routes_through_factory(self, monkeypatch):
        import geoparquet_io.core.benchmark as benchmark

        captured = _spy_factory(monkeypatch, benchmark)

        info = benchmark.get_file_info(GEOJSON_FILE)

        assert "error" not in info, info
        assert info["feature_count"] > 0
        # Local file, and the geometry work needs the spatial extension that
        # the factory loads by default.
        assert captured == [{"load_httpfs": False}]


class TestBenchmarkDuckdb:
    """core/benchmark.py (benchmark_duckdb)."""

    def test_routes_through_factory(self, monkeypatch, tmp_path):
        import geoparquet_io.core.benchmark as benchmark

        captured = _spy_factory(monkeypatch, benchmark)
        output_path = tmp_path / "out.parquet"

        benchmark.benchmark_duckdb(GEOJSON_FILE, output_path)

        assert output_path.exists()
        assert captured == [{"load_httpfs": False}]

    def test_factory_setup_is_outside_the_measured_window(self, monkeypatch, tmp_path):
        """The connection must be built before the timer starts: extension
        install/load is fixed overhead the GeoPandas/pyogrio arms never pay, so
        including it would bias the comparison against DuckDB."""
        import geoparquet_io.core.benchmark as benchmark

        baseline, _ = benchmark.benchmark_duckdb(GEOJSON_FILE, tmp_path / "baseline.parquet")

        slow_setup_seconds = 1.0

        def slow_factory(**kwargs):
            con = real_get_duckdb_connection(**kwargs)
            time.sleep(slow_setup_seconds)
            return con

        monkeypatch.setattr(benchmark, "get_duckdb_connection", slow_factory)

        elapsed, _ = benchmark.benchmark_duckdb(GEOJSON_FILE, tmp_path / "out.parquet")

        # Half the injected delay: well clear of run-to-run jitter on the same
        # COPY, but nowhere near enough to absorb the setup cost.
        assert elapsed < baseline + slow_setup_seconds / 2


class TestWkbToWktPreview:
    """core/inspect_utils.py (wkb_to_wkt_preview)."""

    def test_routes_through_factory(self, monkeypatch):
        import geoparquet_io.core.inspect_utils as inspect_utils

        captured = _spy_factory(monkeypatch, inspect_utils)

        wkt = inspect_utils.wkb_to_wkt_preview(WKB_POINT)

        assert "POINT" in wkt
        # Converts bytes already in memory; no file, so no httpfs.
        assert captured == [{"load_httpfs": False}]


class TestGetColumnStatistics:
    """core/inspect_utils.py (get_column_statistics)."""

    def test_routes_through_factory_and_computes_stats(self, monkeypatch, fields_v2_file):
        import geoparquet_io.core.inspect_utils as inspect_utils

        captured = _spy_factory(monkeypatch, inspect_utils)

        columns_info = [{"name": "id", "is_geometry": False}]
        stats = inspect_utils.get_column_statistics(fields_v2_file, columns_info)

        # Per-column failures are swallowed into a default dict, so asserting
        # the key exists proves nothing — assert the computed values.
        assert stats["id"]["unique"] == 91
        assert stats["id"]["nulls"] == 0
        assert captured == [{"load_httpfs": False}]

    def test_requests_httpfs_for_a_remote_file(self, monkeypatch):
        """A local file needs no httpfs, but a cloud-storage one does — the
        sibling get_preview_data has always decided this per file, and this
        function used to hardcode it off, so `inspect stats` on an s3:// file
        could not open it."""
        import geoparquet_io.core.inspect_utils as inspect_utils

        captured = _kwargs_only_spy(monkeypatch, inspect_utils)

        # Empty columns_info: the connection is built before the per-column
        # loop, so no query (and no network access) is ever issued.
        inspect_utils.get_column_statistics("s3://example-bucket/data.parquet", [])

        assert captured == [{"load_httpfs": True}]


class TestCountryCodesCreateConnection:
    """core/add/country_codes.py (_create_duckdb_connection)."""

    def test_local_countries_file_needs_no_cloud_access(self, monkeypatch):
        import geoparquet_io.core.add.country_codes as country_codes

        captured = _spy_factory(monkeypatch, country_codes)

        with country_codes._create_duckdb_connection(using_default=False):
            pass

        assert captured == [{"load_httpfs": False}]

    def test_using_default_reads_overture_over_httpfs(self, monkeypatch):
        """The default countries source is the remote Overture release on S3,
        so it needs both httpfs and the bucket's region. Asking for the region
        while declaring load_httpfs=False was self-contradictory: DuckDB
        autoloads httpfs on `SET s3_region` anyway."""
        import geoparquet_io.core.add.country_codes as country_codes

        captured = _spy_factory(monkeypatch, country_codes)

        with country_codes._create_duckdb_connection(using_default=True) as con:
            region = con.execute("SELECT current_setting('s3_region')").fetchone()[0]

        assert captured == [{"load_httpfs": True, "s3_region": "us-west-2"}]
        assert region == "us-west-2"


class TestDiskRewriteWriteFromTable:
    """core/write_strategies/disk_rewrite.py (write_from_table)."""

    def test_routes_through_factory(self, monkeypatch, tmp_path):
        import geoparquet_io.core.write_strategies.disk_rewrite as disk_rewrite

        captured = _spy_factory(monkeypatch, disk_rewrite)

        table = pa.table({"id": [1, 2, 3], "geometry": [WKB_POINT] * 3})
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
        # Writes to a local temp file; the input is an in-memory Arrow table.
        assert captured == [{"load_httpfs": False}]


def _bash_can_run_scripts() -> bool:
    """True when ``bash`` on PATH can actually execute a script.

    ``shutil.which("bash")`` is not enough on Windows runners: there ``bash``
    resolves to WSL's ``bash.exe``, which exits non-zero with an "install a
    distribution" notice when no WSL distro is present. That makes every hook
    invocation return 1 -- failing the tests that expect a clean tree, and
    passing the rejection tests for entirely the wrong reason.
    """
    try:
        probe = subprocess.run(
            ["bash", "-c", "exit 0"],
            capture_output=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return probe.returncode == 0


_NEEDS_BASH = pytest.mark.skipif(
    not _bash_can_run_scripts(),
    reason="pre-commit hook scripts are POSIX shell; no usable bash on this platform",
)


@_NEEDS_BASH
class TestAntipatternHookBansBareConnect:
    """Self-test that the extended duckdb-antipatterns hook rejects a bare
    ``duckdb.connect(`` reintroduced outside core/duckdb_utils.py."""

    @staticmethod
    def _hook_script():
        """Extract the duckdb-antipatterns hook script from
        .pre-commit-config.yaml, exactly as pre-commit would invoke it."""
        config = yaml.safe_load((REPO_ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
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
            encoding="utf-8",
            errors="replace",
        )

    def _write_workspace(self, tmp_path, extra_files):
        """Build an isolated `geoparquet_io/` tree under tmp_path so these
        tests never mutate the real, shared repo tree — this test suite runs
        under pytest-xdist with parallel workers.
        """
        core_dir = tmp_path / "geoparquet_io" / "core"
        core_dir.mkdir(parents=True)
        # A minimal stand-in for the factory's own internal connect, which
        # must stay exempt.
        (core_dir / "duckdb_utils.py").write_text(
            "def get_duckdb_connection(config=None):\n"
            "    con = duckdb.connect(config=config) if config else duckdb.connect()\n"
            "    return con\n",
            encoding="utf-8",
        )
        for name, content in extra_files.items():
            (core_dir / name).write_text(content, encoding="utf-8")
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

    @pytest.mark.parametrize(
        ("label", "source"),
        [
            ("alias_import", "import duckdb as d\n\ncon = d.connect()\n"),
            ("symbol_import", "from duckdb import connect\n\ncon = connect()\n"),
            ("implicit_sql", 'import duckdb\n\nduckdb.sql("select 1")\n'),
            ("implicit_execute", 'import duckdb\n\nduckdb.execute("select 1")\n'),
            ("implicit_read_parquet", 'import duckdb\n\nduckdb.read_parquet("f")\n'),
        ],
    )
    def test_hook_rejects_evasions_of_the_duckdb_prefix(self, tmp_path, label, source):
        """Aliasing the module, importing `connect` directly, or using DuckDB's
        implicit default connection all bypass the factory just as thoroughly as
        a bare `duckdb.connect()`, so the hook must reject them too."""
        workspace = self._write_workspace(tmp_path, {f"{label}.py": source})
        result = self._run_hook(cwd=workspace)
        assert result.returncode != 0, result.stdout + result.stderr

    def test_hook_honors_the_allow_bare_connect_escape_hatch(self, tmp_path):
        """A deliberate plain connection stays possible, mirroring the
        `# allow-cwd-change` hatch on the no-cwd-change-in-tests hook."""
        workspace = self._write_workspace(
            tmp_path,
            {"deliberate.py": "import duckdb\n\ncon = duckdb.connect()  # allow-bare-connect\n"},
        )
        result = self._run_hook(cwd=workspace)
        assert result.returncode == 0, result.stdout + result.stderr

    def test_hook_does_not_exempt_a_nested_duckdb_utils_module(self, tmp_path):
        """The exemption is for the one real factory module, matched on the
        path field — not any file that happens to be named duckdb_utils.py."""
        nested = tmp_path / "geoparquet_io" / "core" / "partition"
        workspace = self._write_workspace(tmp_path, {})
        nested.mkdir(parents=True)
        (nested / "duckdb_utils.py").write_text(
            "import duckdb\n\ncon = duckdb.connect()\n", encoding="utf-8"
        )
        result = self._run_hook(cwd=workspace)
        assert result.returncode != 0, result.stdout + result.stderr
