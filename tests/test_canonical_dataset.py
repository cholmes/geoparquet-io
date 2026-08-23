"""Meta-tests for the canonical sample dataset in ``tests/data/canonical/``.

The canonical dataset is the shared input for the docs-as-tests harness, the
end-to-end journey tests, and the ``examples/`` notebooks. Those consumers pin
placeholder names (``input.parquet``, ``places.parquet``, ``buildings.parquet``,
``input.geojson``, ...) to these files, so a silent change here would quietly
change the meaning of every downstream test.

These tests pin the shape of the dataset: row counts, exact column lists,
GeoParquet metadata, clean spec validation, cross-representation consistency,
the ``examples/data/`` mirror, and the size budget. Regenerate the dataset with
``uv run python tests/data/canonical/generate_canonical.py`` and update the
constants here deliberately if the dataset is ever meant to change.
"""

from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.validate import CheckStatus, validate_geoparquet

REPO_ROOT = Path(__file__).resolve().parent.parent
CANONICAL_DIR = REPO_ROOT / "tests" / "data" / "canonical"
EXAMPLES_DATA_DIR = REPO_ROOT / "examples" / "data"

PLACES_PARQUET = CANONICAL_DIR / "places.parquet"
BUILDINGS_PARQUET = CANONICAL_DIR / "buildings.parquet"
PLACES_GEOJSON = CANONICAL_DIR / "places.geojson"
PLACES_CSV = CANONICAL_DIR / "places.csv"

DATA_FILES = (PLACES_PARQUET, BUILDINGS_PARQUET, PLACES_GEOJSON, PLACES_CSV)
PARQUET_FILES = (PLACES_PARQUET, BUILDINGS_PARQUET)

EXPECTED_ROWS = {"places.parquet": 766, "buildings.parquet": 42}
EXPECTED_COLUMNS = {
    "places.parquet": [
        "fsq_place_id",
        "name",
        "address",
        "placemaker_url",
        "geometry",
        "bbox",
    ],
    "buildings.parquet": ["id", "geometry", "bbox"],
}
EXPECTED_GEOMETRY_TYPES = {
    "places.parquet": ["Point"],
    "buildings.parquet": ["Polygon"],
}

PLACES_CSV_COLUMNS = [
    "fsq_place_id",
    "name",
    "address",
    "placemaker_url",
    "lon",
    "lat",
]

# Every canonical file is mirrored into examples/data/ so notebooks and docs can
# use the same data without reaching into tests/.
MIRRORED_FILES = (
    "places.parquet",
    "buildings.parquet",
    "places.geojson",
    "places.csv",
)

# The dataset ships in the repository twice (tests/data/canonical + the mirror),
# so the per-copy budget is deliberately tight.
MAX_CANONICAL_BYTES = 1_000_000


GENERATOR = CANONICAL_DIR / "generate_canonical.py"


def _geo_metadata(path: Path) -> dict:
    """Return the parsed ``geo`` key of a parquet file's schema metadata."""
    metadata = pq.ParquetFile(path).schema_arrow.metadata or {}
    return json.loads(metadata[b"geo"])


def _load_generator():
    """Import generate_canonical.py by path - it is a script, not a package."""
    spec = importlib.util.spec_from_file_location("generate_canonical", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("path", DATA_FILES, ids=lambda p: p.name)
def test_canonical_file_exists(path):
    """Every canonical file is present and non-empty."""
    assert path.is_file(), f"missing canonical file: {path}"
    assert path.stat().st_size > 0


def test_regeneration_script_and_readme_exist():
    """The dataset is a reproducible artifact, not a mystery blob."""
    assert (CANONICAL_DIR / "generate_canonical.py").is_file()
    assert (CANONICAL_DIR / "README.md").is_file()


@pytest.mark.parametrize("path", PARQUET_FILES, ids=lambda p: p.name)
def test_parquet_row_count(path):
    """Row counts are pinned so downstream journey assertions stay meaningful."""
    assert pq.ParquetFile(path).metadata.num_rows == EXPECTED_ROWS[path.name]


@pytest.mark.parametrize("path", PARQUET_FILES, ids=lambda p: p.name)
def test_parquet_columns(path):
    """Exact column list, in order - doc examples reference these names."""
    assert pq.ParquetFile(path).schema_arrow.names == EXPECTED_COLUMNS[path.name]


@pytest.mark.parametrize("path", PARQUET_FILES, ids=lambda p: p.name)
def test_parquet_geo_metadata(path):
    """GeoParquet 1.1 metadata with a bbox covering, ready for spatial pushdown."""
    geo = _geo_metadata(path)
    assert geo["version"].startswith("1.1")
    assert geo["primary_column"] == "geometry"

    column = geo["columns"]["geometry"]
    assert column["encoding"] == "WKB"
    assert column["geometry_types"] == EXPECTED_GEOMETRY_TYPES[path.name]
    assert column["covering"]["bbox"] == {
        "xmin": ["bbox", "xmin"],
        "ymin": ["bbox", "ymin"],
        "xmax": ["bbox", "xmax"],
        "ymax": ["bbox", "ymax"],
    }


@pytest.mark.parametrize("path", PARQUET_FILES, ids=lambda p: p.name)
def test_parquet_passes_spec_validation(path):
    """gpio's own validator reports a clean bill of health on its sample data."""
    result = validate_geoparquet(str(path), validate_data=True, sample_size=0)

    problems = [
        f"{check.status.value}: {check.name} - {check.message}"
        for check in result.checks
        if check.status in (CheckStatus.FAILED, CheckStatus.WARNING)
    ]
    assert not problems, f"{path.name} failed spec validation: {problems}"
    assert result.detected_version.startswith("1.1")
    assert result.passed_count > 0


@pytest.mark.parametrize("path", PARQUET_FILES, ids=lambda p: p.name)
def test_parquet_compression_is_zstd(path):
    """ZSTD keeps `gpio check compression` clean on the canonical files."""
    metadata = pq.ParquetFile(path).metadata
    codecs = {
        metadata.row_group(rg).column(col).compression
        for rg in range(metadata.num_row_groups)
        for col in range(metadata.num_columns)
    }
    assert codecs == {"ZSTD"}


def test_geojson_shape():
    """The GeoJSON derivative is a FeatureCollection of the same 766 points."""
    document = json.loads(PLACES_GEOJSON.read_text(encoding="utf-8"))

    assert document["type"] == "FeatureCollection"
    assert len(document["features"]) == EXPECTED_ROWS["places.parquet"]

    first = document["features"][0]
    assert first["type"] == "Feature"
    assert first["geometry"]["type"] == "Point"
    assert len(first["geometry"]["coordinates"]) == 2
    # bbox is a GeoParquet covering, not a GeoJSON property - it is dropped.
    assert list(first["properties"]) == [
        "fsq_place_id",
        "name",
        "address",
        "placemaker_url",
    ]


def test_csv_shape():
    """The CSV derivative carries lon/lat columns, ready for `convert geoparquet`."""
    with PLACES_CSV.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    assert list(rows[0]) == PLACES_CSV_COLUMNS
    assert len(rows) == EXPECTED_ROWS["places.parquet"]

    lons = [float(row["lon"]) for row in rows]
    lats = [float(row["lat"]) for row in rows]
    # Northern Ghana / Togo border region - the source extract's footprint.
    assert -1.0 < min(lons) and max(lons) < 1.5
    assert 9.7 < min(lats) and max(lats) < 12.5


def test_derivatives_describe_the_same_features():
    """Parquet, GeoJSON and CSV agree on identity and position, row for row."""
    table = pq.read_table(PLACES_PARQUET)
    parquet_ids = table.column("fsq_place_id").to_pylist()

    features = json.loads(PLACES_GEOJSON.read_text(encoding="utf-8"))["features"]
    geojson_ids = [f["properties"]["fsq_place_id"] for f in features]

    with PLACES_CSV.open(newline="", encoding="utf-8") as handle:
        csv_rows = list(csv.DictReader(handle))
    csv_ids = [row["fsq_place_id"] for row in csv_rows]

    assert geojson_ids == parquet_ids
    assert csv_ids == parquet_ids

    # gpio writes GeoJSON coordinates rounded to 7 decimals (~1 cm).
    for feature, row in zip(features, csv_rows, strict=True):
        lon, lat = feature["geometry"]["coordinates"]
        assert lon == pytest.approx(float(row["lon"]), abs=1e-6)
        assert lat == pytest.approx(float(row["lat"]), abs=1e-6)


@pytest.mark.parametrize("name", MIRRORED_FILES)
def test_examples_mirror_is_byte_identical(name):
    """examples/data/ is a mirror, so regenerating the canonical set updates both."""
    mirrored = EXAMPLES_DATA_DIR / name
    assert mirrored.is_file(), f"missing mirror: {mirrored}"
    assert mirrored.read_bytes() == (CANONICAL_DIR / name).read_bytes()


def test_existing_sample_parquet_is_preserved():
    """Notebooks still reference examples/data/sample.parquet; it must survive."""
    assert (EXAMPLES_DATA_DIR / "sample.parquet").is_file()


def test_canonical_dataset_stays_small():
    """Keep the committed dataset well under a megabyte per copy."""
    total = sum(path.stat().st_size for path in DATA_FILES)
    assert total < MAX_CANONICAL_BYTES, f"canonical dataset grew to {total} bytes"


def test_generator_output_dir_defaults_to_the_canonical_directory():
    """No arguments means "regenerate in place", mirror included."""
    args = _load_generator().parse_args([])

    assert args.output_dir == CANONICAL_DIR
    assert args.mirror is True


def test_generator_output_dir_elsewhere_skips_the_mirror(tmp_path):
    """A regeneration aimed at a temp directory must not write into the repo."""
    args = _load_generator().parse_args(["--output-dir", str(tmp_path)])

    assert args.output_dir == tmp_path.resolve()
    assert args.mirror is False


@pytest.mark.slow
def test_regeneration_reproduces_the_committed_files(tmp_path):
    """The committed dataset is still what the generator produces today.

    Without this, a change in gpio's sort, bbox, GeoJSON or compression output
    would leave the committed files stale and the generator's promise of
    reproducibility quietly false - discovered only by whoever next reran it.
    """
    module = _load_generator()
    examples_before = {name: (EXAMPLES_DATA_DIR / name).read_bytes() for name in MIRRORED_FILES}

    assert module.main(["--output-dir", str(tmp_path)]) == 0

    drifted = [
        name for name in MIRRORED_FILES if _sha256(tmp_path / name) != _sha256(CANONICAL_DIR / name)
    ]
    assert not drifted, (
        f"regenerating produced different bytes for {drifted}; gpio's output "
        f"changed - rerun tests/data/canonical/generate_canonical.py and commit"
    )

    # --output-dir must leave the repository alone.
    for name, content in examples_before.items():
        assert (EXAMPLES_DATA_DIR / name).read_bytes() == content
