"""Build the working directory a documentation example runs in.

The docs never name a real bundled file — they say ``input.parquet``,
``data.parquet``, ``buildings.parquet``. That convention is what makes running
them feasible: instead of editing 400 blocks to point at fixtures, the harness
copies the canonical dataset into a scratch directory *under the placeholder
names the docs already use*, and the example runs verbatim.

Every entry below is justified by a census of the guides. Only *inputs* are
seeded: a name that examples predominantly write to (``output.parquet``,
``cells.parquet``) is deliberately absent, because pre-creating it would mask a
command that never actually produced anything.
"""

from __future__ import annotations

import shutil
from pathlib import Path

CANONICAL_DIR = Path(__file__).resolve().parent.parent / "data" / "canonical"

#: Placeholder name -> canonical file it is seeded from. The comment on each
#: line is roughly how often the guides mention that name.
SEED_FILES: dict[str, str] = {
    # Point workhorse: 766 Foursquare places with name/address attributes.
    "input.parquet": "places.parquet",  # ~202
    "data.parquet": "places.parquet",  # ~180
    "places.parquet": "places.parquet",  # ~8
    "myfile.parquet": "places.parquet",  # ~12
    "sample.parquet": "places.parquet",  # ~6
    "large.parquet": "places.parquet",  # ~8
    "large_file.parquet": "places.parquet",  # ~5
    "huge_dataset.parquet": "places.parquet",  # ~4
    "dense.parquet": "places.parquet",  # ~4
    "roads.parquet": "places.parquet",  # ~6
    "geography.parquet": "places.parquet",  # ~3
    "input_v2.parquet": "places.parquet",  # ~5
    # Polygon workhorse: 42 building footprints.
    "buildings.parquet": "buildings.parquet",  # ~22
    "fields.parquet": "buildings.parquet",  # ~36, agricultural field boundaries
    # Other formats.
    "input.geojson": "places.geojson",  # ~4
    "places.geojson": "places.geojson",
    "data.csv": "places.csv",  # ~3
    "points.csv": "places.csv",
    "roads.geojson": "places.geojson",
    "input.csv": "places.csv",
    "places.csv": "places.csv",
}

#: Directories examples write into and expect to exist.
SEED_DIRS = ("output_dir",)


def seed_workdir(workdir: Path) -> Path:
    """Populate ``workdir`` with the placeholder files the guides assume.

    Returns ``workdir`` so callers can chain. Copies are made with
    :func:`shutil.copyfile`, one per block, which keeps blocks hermetic: a
    example that mangles ``input.parquet`` cannot poison the next one.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    for placeholder, canonical in SEED_FILES.items():
        source = CANONICAL_DIR / canonical
        if source.exists():
            shutil.copyfile(source, workdir / placeholder)
    for name in SEED_DIRS:
        (workdir / name).mkdir(exist_ok=True)
    return workdir


def missing_canonical_files() -> list[str]:
    """Canonical files referenced by :data:`SEED_FILES` that do not exist."""
    return sorted({name for name in SEED_FILES.values() if not (CANONICAL_DIR / name).exists()})
