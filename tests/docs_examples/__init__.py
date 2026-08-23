"""Run the examples in ``docs/guide/*.md`` as tests.

The guides are the project's main promise to users, and every fenced ``bash`` or
``python`` block in them is an implicit claim that the command works. This
package makes pytest collect those blocks as real test items, so the claim is
checked on every run instead of on every complaint.

Three pieces:

``parser``
    Finds fenced blocks — including the 4-space-indented fences inside
    ``pymdownx`` tabbed sections (``=== "CLI"``) that off-the-shelf markdown
    doctest plugins cannot see — and reads the ``<!-- doctest: ... -->``
    directives that sit above them.
``seeder``
    Builds a hermetic working directory per block, populated from
    ``tests/data/canonical/`` under the placeholder names the docs already use
    (``input.parquet``, ``data.parquet``, ``buildings.parquet``, ...), which is
    what lets most examples run verbatim.
``collector``
    The pytest glue: one item per block, ids like ``guide/sort.md:L14[bash]``.

The entry point is ``pytest_collect_file`` in the repository-root
``conftest.py``; ``docs/guide`` is on ``testpaths`` so a plain ``pytest`` run
picks the blocks up.
"""

from tests.docs_examples.parser import (
    DIRECTIVE_KEYWORDS,
    EXECUTABLE_LANGUAGES,
    INERT_LANGUAGES,
    Block,
    Directives,
    DirectiveSyntaxError,
    iter_blocks,
    iter_fences,
    strip_prompts,
)
from tests.docs_examples.seeder import SEED_FILES, seed_workdir

__all__ = [
    "DIRECTIVE_KEYWORDS",
    "EXECUTABLE_LANGUAGES",
    "INERT_LANGUAGES",
    "SEED_FILES",
    "Block",
    "Directives",
    "DirectiveSyntaxError",
    "iter_blocks",
    "iter_fences",
    "seed_workdir",
    "strip_prompts",
]
