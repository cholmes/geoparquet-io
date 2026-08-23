"""Keep nbmake from claiming a green check for notebooks it cannot execute.

The ``notebooks`` CI job runs ``pytest --nbmake examples/*.ipynb``. Any notebook
listed here is skipped, because every operation it demonstrates needs
credentials or network access that CI does not have. Running it would either
fail or -- as happened with ``05_cloud_workflows.ipynb`` before issue #667 --
pass while executing nothing, which is worse: a false positive.

Excluding a notebook is a claim that CI proves nothing about it, so each entry
must also say so in its own prose via an ``<!-- nbsignal: illustrative ... -->``
directive. ``tests/test_notebook_signal.py`` enforces both halves.

Implemented as a skip in ``pytest_collection_modifyitems`` rather than
``collect_ignore``: ``collect_ignore`` is only consulted when pytest walks a
directory, and the CI job passes an expanded ``examples/*.ipynb`` file list, for
which it is silently bypassed. A skip also beats a deselect here, because the
reason is printed in the CI log instead of the notebook just vanishing from the
report.
"""

import pytest

#: Notebooks nbmake must not execute. Keep this list as short as the truth allows.
ILLUSTRATIVE_NOTEBOOKS = {
    # Every cell uploads to S3/GCS/Azure. Live-cloud testing (a MinIO service
    # container) is explicitly out of scope for #667.
    "05_cloud_workflows.ipynb": (
        "illustrative only: every cell uploads to cloud storage and needs live "
        "credentials. See the note at the top of the notebook."
    ),
}


def pytest_collection_modifyitems(items):
    """Skip the illustrative notebooks however they were passed to pytest."""
    for item in items:
        reason = ILLUSTRATIVE_NOTEBOOKS.get(item.path.name)
        if reason is not None:
            item.add_marker(pytest.mark.skip(reason=reason))
