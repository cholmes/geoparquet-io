"""Regression tests for library-mode state leaks.

Three distinct global-side-effect bugs that make gpio unsafe to embed:

- F1: write_parquet_with_metadata mutated the caller-supplied
  ``extra_kv_metadata`` dict in place, so a dict reused across writes
  accumulated preserved keys from prior files.
- F2: the S3 write path set ``AWS_PROFILE`` in the process environment
  without ever restoring it, so a single ``.write(profile=...)`` bled the
  profile into every later, unrelated call.
- F3: ``configure_verbose(True)`` raised the shared logger to DEBUG but
  ``configure_verbose(False)`` never lowered it again (one-way ratchet).
"""

import logging
import os
from unittest.mock import patch

import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# F1 — write_parquet_with_metadata must not mutate the caller's dict
# ---------------------------------------------------------------------------


def test_write_does_not_mutate_caller_extra_kv_dict(buildings_test_file, tmp_path):
    """Reusing one extra_kv_metadata dict across writes must not accumulate keys."""
    from geoparquet_io.core.common import write_parquet_with_metadata
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    query = f"SELECT * FROM read_parquet('{buildings_test_file}')"
    caller_dict: dict[str, str] = {}
    out_a = tmp_path / "out_a.parquet"
    out_b = tmp_path / "out_b.parquet"

    try:
        write_parquet_with_metadata(
            con,
            query,
            str(out_a),
            original_metadata={"custom_a": '{"x": 1}'},
            extra_kv_metadata=caller_dict,
            geoparquet_version="1.1",
        )
        assert caller_dict == {}, "caller dict mutated after first write"

        write_parquet_with_metadata(
            con,
            query,
            str(out_b),
            original_metadata={"custom_b": '{"y": 2}'},
            extra_kv_metadata=caller_dict,
            geoparquet_version="1.1",
        )
        assert caller_dict == {}, "caller dict mutated after second write"
    finally:
        con.close()

    # Each output still carries its own preserved key, with no cross-file leak.
    meta_a = pq.read_metadata(str(out_a)).metadata
    meta_b = pq.read_metadata(str(out_b)).metadata
    assert b"custom_a" in meta_a
    assert b"custom_b" in meta_b
    assert b"custom_b" not in meta_a, "second file's key leaked into the first"
    assert b"custom_a" not in meta_b, "first file's key leaked into the second"


# ---------------------------------------------------------------------------
# F2 — the S3 write path must restore AWS_PROFILE (incl. the was-unset case)
# ---------------------------------------------------------------------------


def test_api_write_to_s3_restores_set_aws_profile(buildings_test_file, monkeypatch):
    """A profile= write to S3 must restore a pre-existing AWS_PROFILE."""
    import geoparquet_io as gpio

    monkeypatch.setenv("AWS_PROFILE", "original-profile")
    with patch("geoparquet_io.core.upload.upload") as mock_upload:
        gpio.read(buildings_test_file).write(
            "s3://fake-bucket/out.parquet", profile="write-profile"
        )
    assert mock_upload.called, "upload should have been invoked for the remote write"
    assert os.environ.get("AWS_PROFILE") == "original-profile"


def test_api_write_to_s3_restores_unset_aws_profile(buildings_test_file, monkeypatch):
    """A profile= write to S3 must leave AWS_PROFILE unset if it started unset."""
    import geoparquet_io as gpio

    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with patch("geoparquet_io.core.upload.upload") as mock_upload:
        gpio.read(buildings_test_file).write(
            "s3://fake-bucket/out.parquet", profile="write-profile"
        )
    assert mock_upload.called
    assert "AWS_PROFILE" not in os.environ


def test_aws_profile_scope_restores_set_value(monkeypatch):
    """aws_profile_scope sets the profile inside and restores the prior value."""
    from geoparquet_io.core.remote import aws_profile_scope

    monkeypatch.setenv("AWS_PROFILE", "before")
    with aws_profile_scope("inside", "s3://bucket/key.parquet"):
        assert os.environ["AWS_PROFILE"] == "inside"
    assert os.environ["AWS_PROFILE"] == "before"


def test_aws_profile_scope_restores_unset(monkeypatch):
    """aws_profile_scope removes the var again if it was unset to begin with."""
    from geoparquet_io.core.remote import aws_profile_scope

    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with aws_profile_scope("inside", "s3://bucket/key.parquet"):
        assert os.environ["AWS_PROFILE"] == "inside"
    assert "AWS_PROFILE" not in os.environ


def test_aws_profile_scope_noop_for_local_path(monkeypatch):
    """No S3 path means no env mutation at all."""
    from geoparquet_io.core.remote import aws_profile_scope

    monkeypatch.setenv("AWS_PROFILE", "before")
    with aws_profile_scope("inside", "/tmp/local.parquet"):
        assert os.environ["AWS_PROFILE"] == "before"
    assert os.environ["AWS_PROFILE"] == "before"


# ---------------------------------------------------------------------------
# F3 — configure_verbose must be symmetric (no one-way ratchet to DEBUG)
# ---------------------------------------------------------------------------


def test_configure_verbose_is_symmetric():
    """configure_verbose(False) must lower the level again after a True call."""
    from geoparquet_io.core.logging_config import (
        configure_verbose,
        get_logger,
        setup_cli_logging,
    )

    setup_cli_logging(verbose=False)
    configure_verbose(True)
    assert get_logger().level == logging.DEBUG

    configure_verbose(False)
    assert get_logger().level != logging.DEBUG
    assert get_logger().level == logging.INFO


def test_verbose_call_does_not_leave_later_default_verbose():
    """A verbose call must not make a later default-verbosity call emit DEBUG."""
    from geoparquet_io.core.logging_config import (
        configure_verbose,
        get_logger,
        setup_cli_logging,
    )

    setup_cli_logging(verbose=False)
    configure_verbose(True)  # e.g. one API call opts into verbose
    configure_verbose(False)  # a later, unrelated default call
    assert get_logger().level == logging.INFO
