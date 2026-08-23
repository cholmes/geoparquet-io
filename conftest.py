"""Repository-root pytest configuration.

Exists for one reason: ``pytest_collect_file`` has to be visible from the
directory being collected, and the documentation examples live in ``docs/guide``,
outside ``tests/``. Everything else stays in ``tests/conftest.py``.
"""

from __future__ import annotations

from pathlib import Path

DOCS_GUIDE = Path(__file__).parent / "docs" / "guide"


def pytest_collect_file(parent, file_path: Path):
    """Collect ``docs/guide/*.md`` as documentation-example test files."""
    if file_path.suffix != ".md" or file_path.parent != DOCS_GUIDE:
        return None
    from tests.docs_examples.collector import DocExampleFile

    return DocExampleFile.from_parent(parent, path=file_path)
