#!/usr/bin/env python3
"""Emit pip-audit --ignore-vuln args from the .pip-audit-ignores file.

Single source of truth for pip-audit ignores, consumed by both the tests.yml
security job and the security-audit.yml workflow:

    uv run pip-audit $(uv run python scripts/pip_audit_ignores.py)

File format (whitespace-separated, ``#`` starts a comment line):

    VULN-ID  EXPIRES(YYYY-MM-DD)  REASON...

Entries past their expiry date are dropped, so a still-present vulnerability
starts failing CI again automatically instead of being ignored forever.
Malformed lines fail loudly — a broken ignore file must never silently
disable auditing.
"""

import datetime
import sys
from pathlib import Path

DEFAULT_PATH = Path(__file__).parent.parent / ".pip-audit-ignores"


class IgnoreFileError(ValueError):
    """Raised when .pip-audit-ignores contains a malformed line."""


def active_ignores(path: Path, today: datetime.date) -> list[str]:
    """Return non-expired vulnerability IDs from the ignore file."""
    ids: list[str] = []
    for lineno, raw in enumerate(path.read_text().splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(maxsplit=2)
        if len(parts) < 3:
            raise IgnoreFileError(
                f"{path.name} line {lineno}: expected 'VULN-ID EXPIRES REASON', got: {raw!r}"
            )
        vuln_id, expires_str = parts[0], parts[1]
        try:
            expires = datetime.date.fromisoformat(expires_str)
        except ValueError as exc:
            raise IgnoreFileError(
                f"{path.name} line {lineno}: expiry must be YYYY-MM-DD, got: {expires_str!r}"
            ) from exc
        if today <= expires:
            ids.append(vuln_id)
    return ids


def format_args(ids: list[str]) -> str:
    """Format vulnerability IDs as pip-audit --ignore-vuln arguments."""
    return " ".join(f"--ignore-vuln {vuln_id}" for vuln_id in ids)


def main() -> int:
    try:
        ids = active_ignores(DEFAULT_PATH, today=datetime.date.today())
    except (OSError, IgnoreFileError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    output = format_args(ids)
    if output:
        print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
