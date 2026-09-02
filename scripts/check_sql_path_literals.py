#!/usr/bin/env python3
"""Ratchet: no NEW hand-written ``FROM '{path}'`` SQL interpolations.

Writing the quotes by hand is what let ``gpio add admin-divisions`` interpolate
an unescaped CLI argument and die with a ``ParserException`` on an output path
containing an apostrophe -- after it had already written the file (issue #718).
The fix is :func:`geoparquet_io.core.duckdb_utils.sql_path`, which returns a
complete, quoted, escaped literal::

    FROM {sql_path(path)}          # good -- one escape, no hand-written quotes
    FROM '{path}'                  # bad  -- the escape is the author's problem

Most of the existing sites are correct: they interpolate a ``safe_file_url``
result, which is already escaped. Rewriting all of them at once would be churn
on working code and unreviewable, so this is a **ratchet**, not a ban: each
file's current count is recorded in the baseline, and the check fails only when
a file's count *goes up* (or a file not in the baseline grows one). Lowering a
count is always allowed -- run ``--update`` after migrating call sites.

Usage::

    uv run python scripts/check_sql_path_literals.py            # check
    uv run python scripts/check_sql_path_literals.py --update   # rewrite baseline
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE = REPO_ROOT / "scripts" / "sql_path_literals_baseline.txt"
SCAN_ROOTS = ("geoparquet_io", "plugins")

# A file path interpolated straight into a SQL string literal: the opening quote
# and the ``{`` are adjacent, so the author -- not a helper -- owns the escape.
PATTERN = re.compile(
    r"""(?ix)
    (?:
        \b (?: FROM | TO ) \s
      | \b (?: read_parquet
             | read_csv(?:_auto)?
             | read_json(?:_auto)?
             | read_blob
             | ST_Read(?:_Meta)?
             | parquet_schema
             | parquet_metadata
             | parquet_file_metadata
             | parquet_kv_metadata
             | glob
          ) \(
    )
    '\{
    """
)


def source_files() -> list[Path]:
    files: list[Path] = []
    for root in SCAN_ROOTS:
        files.extend(sorted((REPO_ROOT / root).rglob("*.py")))
    return files


def count_in(path: Path) -> int:
    text = path.read_text(encoding="utf-8")
    return sum(
        1
        for line in text.splitlines()
        if not line.lstrip().startswith("#") and PATTERN.search(line)
    )


def scan() -> dict[str, int]:
    counts = {}
    for path in source_files():
        n = count_in(path)
        if n:
            counts[path.relative_to(REPO_ROOT).as_posix()] = n
    return counts


def read_baseline() -> dict[str, int]:
    if not BASELINE.exists():
        return {}
    baseline = {}
    for line in BASELINE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        count, _, rel = line.partition(" ")
        baseline[rel.strip()] = int(count)
    return baseline


def write_baseline(counts: dict[str, int]) -> None:
    header = (
        "# Hand-written `FROM '{path}'` SQL interpolations per file (issue #718).\n"
        "# A ratchet, not a target: counts may fall, never rise. New code must use\n"
        "# sql_path() from geoparquet_io/core/duckdb_utils.py instead.\n"
        "# Regenerate with: uv run python scripts/check_sql_path_literals.py --update\n"
    )
    body = "".join(f"{n} {rel}\n" for rel, n in sorted(counts.items()))
    BASELINE.write_text(header + body, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update", action="store_true", help="rewrite the baseline")
    args = parser.parse_args()

    counts = scan()
    if args.update:
        write_baseline(counts)
        print(f"Wrote {BASELINE.relative_to(REPO_ROOT)} ({sum(counts.values())} occurrences).")
        return 0

    baseline = read_baseline()
    regressions = {
        rel: (baseline.get(rel, 0), n) for rel, n in counts.items() if n > baseline.get(rel, 0)
    }
    if not regressions:
        return 0

    print("ERROR: new hand-written SQL path interpolation.")
    print("       Use sql_path(path) from core/duckdb_utils.py:")
    print("           FROM {sql_path(path)}      instead of      FROM '{path}'")
    print("       It quotes and escapes in one step, so the escape cannot be forgotten.")
    print("       If the value is already a safe_file_url() result, pass the RAW path here")
    print("       instead and drop the safe_file_url() call -- escaping is not idempotent.")
    print()
    for rel, (was, now) in sorted(regressions.items()):
        print(f"  {rel}: {was} -> {now}")
    print()
    print("       Deliberately keeping an old-style site? Lower the count elsewhere, or")
    print("       run: uv run python scripts/check_sql_path_literals.py --update")
    return 1


if __name__ == "__main__":
    sys.exit(main())
