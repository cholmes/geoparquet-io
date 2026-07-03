"""Tests for scripts/pip_audit_ignores.py — the centralized pip-audit ignore list.

The ignore list lives in .pip-audit-ignores at the repo root and is consumed by
both the tests.yml security job and the security-audit.yml workflow, replacing
the inline bash version-compare hack that previously drifted between the two.
"""

import datetime
from pathlib import Path

import pytest

from scripts.pip_audit_ignores import IgnoreFileError, active_ignores, format_args

REPO_ROOT = Path(__file__).parent.parent
TODAY = datetime.date(2026, 7, 3)


def write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / ".pip-audit-ignores"
    path.write_text(content)
    return path


class TestActiveIgnores:
    def test_valid_line_parses(self, tmp_path):
        path = write(tmp_path, "PYSEC-2026-196 2026-08-15 pip 26.1.1; fixed in 26.1.2\n")
        assert active_ignores(path, today=TODAY) == ["PYSEC-2026-196"]

    def test_comments_and_blank_lines_skipped(self, tmp_path):
        path = write(
            tmp_path,
            "# vuln-id  expires  reason\n"
            "\n"
            "   \n"
            "# another comment\n"
            "GHSA-abcd-1234-efgh 2099-01-01 some reason here\n",
        )
        assert active_ignores(path, today=TODAY) == ["GHSA-abcd-1234-efgh"]

    def test_expired_entries_dropped(self, tmp_path):
        path = write(
            tmp_path,
            "PYSEC-2020-001 2020-01-01 long expired\nPYSEC-2026-196 2026-08-15 still active\n",
        )
        assert active_ignores(path, today=TODAY) == ["PYSEC-2026-196"]

    def test_entry_active_on_expiry_date_itself(self, tmp_path):
        path = write(tmp_path, "PYSEC-2026-001 2026-07-03 expires today, still active\n")
        assert active_ignores(path, today=TODAY) == ["PYSEC-2026-001"]

    def test_missing_reason_rejected(self, tmp_path):
        path = write(tmp_path, "PYSEC-2026-196 2026-08-15\n")
        with pytest.raises(IgnoreFileError, match="line 1"):
            active_ignores(path, today=TODAY)

    def test_malformed_date_rejected(self, tmp_path):
        path = write(tmp_path, "PYSEC-2026-196 15-08-2026 bad date format\n")
        with pytest.raises(IgnoreFileError, match="line 1"):
            active_ignores(path, today=TODAY)

    def test_empty_file_yields_no_ignores(self, tmp_path):
        path = write(tmp_path, "# only comments\n")
        assert active_ignores(path, today=TODAY) == []


class TestFormatArgs:
    def test_formats_ignore_vuln_flags(self):
        assert format_args(["A", "B"]) == "--ignore-vuln A --ignore-vuln B"

    def test_empty_list_formats_to_empty_string(self):
        assert format_args([]) == ""


class TestRepoIgnoreFile:
    def test_repo_ignore_file_parses_cleanly(self):
        """The committed .pip-audit-ignores must always parse."""
        path = REPO_ROOT / ".pip-audit-ignores"
        assert path.exists()
        # Must not raise, regardless of what is currently listed or expired.
        active_ignores(path, today=datetime.date.today())
