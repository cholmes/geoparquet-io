"""Characterization suite: the geo-metadata readers must agree (#664, suite 2).

`gpio` parses the GeoParquet ``geo`` key-value metadata in five places. This
suite pins the *current* behavior so the planned metadata dedup is verifiable:
if consolidating them changes what any caller sees, a test here fails and names
the contract that broke.

The five readers, as inventoried in #664:

1. ``core/geo_metadata.py::parse_geo_metadata`` — bytes keys only
2. ``core/crs_utils.py::parse_geo_metadata_from_schema`` — bytes or str keys
3. ``core/duckdb_metadata.py::get_geo_metadata`` — pyarrow fast path / DuckDB remote
4. ``core/common.py::get_parquet_metadata`` — the kv leg of the returned tuple
5. ``core/add/quadkey.py::_parse_geo_metadata_from_schema`` — private duplicate

Drift since #664 was written: reader 5 is no longer an independent
implementation — it is a one-line delegation to reader 2. It is still exercised
here (callers reach the parse through it, and the consolidation may delete it),
and ``test_quadkey_reader_delegates_to_crs_utils`` pins that alias relationship
so the inventory above stays honest.

Readers 1, 2 and 5 take an Arrow schema metadata dict; readers 3 and 4 take a
path. ``READERS`` adapts both families to one ``(path, schema_metadata)``
signature so a single parametrized test can compare all five.

Two things are deliberately *not* asserted equal:

- The edge-case matrix (``test_edge_case_reader_divergence``). The readers
  legitimately disagree there today; each verdict is pinned individually.
- The CRS-equality trio (``_crs_equals`` / ``_is_crs84_equivalent`` /
  ``_crs_are_equivalent``). Cells where the three disagree are flagged in
  ``CRS_DECISIONS`` as input to the consolidation decision list.
"""

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.quadkey import (
    _parse_geo_metadata_from_schema as read_quadkey,
)
from geoparquet_io.core.common import get_parquet_metadata
from geoparquet_io.core.crs_utils import (
    parse_geo_metadata_from_schema as read_crs_utils,
)
from geoparquet_io.core.duckdb_metadata import get_geo_metadata as read_duckdb
from geoparquet_io.core.geo_metadata import parse_geo_metadata as read_geo_metadata
from geoparquet_io.core.inspect_utils import _crs_are_equivalent
from geoparquet_io.core.validate import _crs_equals, _is_crs84_equivalent

DATA = Path(__file__).parent / "data"
CORPUS = DATA / "geoparquet-testing"


# =============================================================================
# Reader adapters
# =============================================================================


def _read_common_kv(path: Path, _metadata: dict | None) -> dict | None:
    """Reader 4: parse the ``geo`` entry out of ``get_parquet_metadata``'s kv leg.

    ``get_parquet_metadata`` returns raw kv bytes rather than a parsed dict, so
    the JSON decode that the other four readers do internally happens here. That
    decode is part of what the consolidation would absorb.
    """
    kv, _schema = get_parquet_metadata(str(path))
    raw = (kv or {}).get(b"geo")
    if raw is None:
        return None
    return json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)


READERS = {
    "geo_metadata.parse_geo_metadata": lambda path, md: read_geo_metadata(md),
    "crs_utils.parse_geo_metadata_from_schema": lambda path, md: read_crs_utils(md),
    "duckdb_metadata.get_geo_metadata": lambda path, md: read_duckdb(str(path)),
    "common.get_parquet_metadata[kv]": _read_common_kv,
    "add.quadkey._parse_geo_metadata_from_schema": lambda path, md: read_quadkey(md),
}


def _normalize(parsed: dict | None) -> str:
    """Order-insensitive comparable form; key order must not count as divergence."""
    return json.dumps(parsed, sort_keys=True)


def _schema_metadata(path: Path) -> dict | None:
    return pq.ParquetFile(path).schema_arrow.metadata


def _assert_readers_agree(path: Path) -> None:
    """Every reader must produce the same parsed ``geo`` dict for ``path``."""
    metadata = _schema_metadata(path)
    results = {name: _normalize(reader(path, metadata)) for name, reader in READERS.items()}
    distinct = set(results.values())
    assert len(distinct) == 1, f"geo-metadata readers disagree on {path.name}:\n" + "\n".join(
        f"  {name}: {value}" for name, value in sorted(results.items())
    )


# =============================================================================
# Fixture enumeration (at collection time)
# =============================================================================


def _has_geo_key(path: Path) -> bool:
    metadata = _schema_metadata(path)
    return bool(metadata) and b"geo" in metadata


def _local_fixtures() -> list[Path]:
    """Every non-corpus parquet fixture under tests/data/."""
    return sorted(p for p in DATA.rglob("*.parquet") if CORPUS not in p.parents)


GEO_FIXTURES = [p for p in _local_fixtures() if _has_geo_key(p)]
NON_GEO_FIXTURES = [p for p in _local_fixtures() if not _has_geo_key(p)]

# Representative corpus slice: one file per corpus dimension that could plausibly
# change how the ``geo`` blob is written (bbox covering, each CRS spelling, edge
# interpolation, native logical types, coordinate epoch, geometry collections,
# multiple geometry columns, ZM, and a real-world sample).
CORPUS_SAMPLE = [
    "data/bbox/bbox-present.parquet",
    "data/crs/crs-default.parquet",
    "data/crs/crs-epsg-3857.parquet",
    "data/crs/crs-projjson-full.parquet",
    "data/edges/edges-spherical.parquet",
    "data/encodings/point-native-geography.parquet",
    "data/epoch/epoch-itrf2014-2020.parquet",
    "data/geometry_types/geometrycollection.parquet",
    "data/multi_geometry/two-geom-columns-different-crs.parquet",
    "data/zm/linestring-xyzm-native-geometry.parquet",
    "samples/us-states.parquet",
]

# Floor just under the current count (16 geo fixtures). An emptied or moved
# tests/data would otherwise make rglob return [] and pytest's empty-parameter-set
# default would silently SKIP the whole parity test instead of failing.
MIN_GEO_FIXTURES = 14


def test_geo_fixture_inventory_is_populated():
    """Guard: a vanished fixture set must fail loudly, not silently skip."""
    assert len(GEO_FIXTURES) >= MIN_GEO_FIXTURES, (
        f"expected >= {MIN_GEO_FIXTURES} geo fixtures under {DATA}, "
        f"found {len(GEO_FIXTURES)}: {[p.name for p in GEO_FIXTURES]}"
    )


# =============================================================================
# 1. Reader parity
# =============================================================================


@pytest.mark.parametrize("fixture", GEO_FIXTURES, ids=lambda p: p.name)
def test_readers_agree_on_local_fixtures(fixture):
    """All five readers parse every geo fixture to the identical dict."""
    _assert_readers_agree(fixture)


@pytest.mark.parametrize("fixture", NON_GEO_FIXTURES, ids=lambda p: p.name)
def test_readers_agree_on_non_geo_fixtures(fixture):
    """Files with no ``geo`` key: every reader returns None, none of them raise.

    These are parquet-geo-native and plain-parquet fixtures. This is the
    file-backed half of the "absent geo key" edge case, and it is the only place
    readers 3 and 4 (path-based) can be exercised against absence at all.
    """
    metadata = _schema_metadata(fixture)
    for name, reader in READERS.items():
        assert reader(fixture, metadata) is None, f"{name} returned non-None for {fixture.name}"


@pytest.mark.corpus
@pytest.mark.parametrize("relative_path", CORPUS_SAMPLE)
def test_readers_agree_on_corpus_files(relative_path):
    """The same parity holds on the official geoparquet-testing corpus."""
    if not (CORPUS / "data").exists():
        pytest.skip("run: git submodule update --init")
    path = CORPUS / relative_path
    assert path.exists(), f"corpus file moved or removed: {relative_path}"
    _assert_readers_agree(path)


def test_quadkey_reader_delegates_to_crs_utils():
    """Reader 5 is an alias of reader 2, not a fifth implementation.

    #664 inventoried it as a "private duplicate"; it has since been reduced to a
    one-line delegation. Pinned so the consolidation's inventory stays accurate
    and so a future re-divergence is caught.
    """
    assert read_quadkey.__module__ == "geoparquet_io.core.add.quadkey"
    metadata = {"geo": '{"version": "1.1.0", "columns": {}}'}
    assert read_quadkey(metadata) == read_crs_utils(metadata)


# =============================================================================
# 2. Edge cases — the readers legitimately DIVERGE here
# =============================================================================

# Each cell is this reader's CURRENT behavior, pinned individually. Readers 3 and
# 4 are path-based and cannot accept a raw dict, so only the three dict readers
# appear. ``None`` means "returned None"; a dict means "parsed to that dict".
#
# CONSOLIDATION DECISION (from #664): the consolidated reader should adopt
# reader-2 semantics — accept str keys and str values. Reader 1's bytes-only
# strictness is the pre-fix state recorded here, not a contract to preserve.
STR_KEY_META = {"geo": '{"version": "1.1.0", "columns": {}}'}
PARSED_STR_KEY = {"version": "1.1.0", "columns": {}}

EDGE_CASES = {
    # name: (metadata_in, {reader_key: expected_out})
    "str_key_str_value": (
        STR_KEY_META,
        {
            # Reader 1 checks ``b"geo" not in metadata`` and bails: str keys are
            # invisible to it. Divergence to be resolved by adopting reader 2.
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": PARSED_STR_KEY,
            "add.quadkey._parse_geo_metadata_from_schema": PARSED_STR_KEY,
        },
    ),
    "bytes_key_invalid_json": (
        {b"geo": b"{not valid json"},
        {
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": None,
            "add.quadkey._parse_geo_metadata_from_schema": None,
        },
    ),
    "str_key_invalid_json": (
        {"geo": "{not valid json"},
        {
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": None,
            "add.quadkey._parse_geo_metadata_from_schema": None,
        },
    ),
    "absent_geo_key": (
        {b"other": b"value"},
        {
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": None,
            "add.quadkey._parse_geo_metadata_from_schema": None,
        },
    ),
    "empty_dict": (
        {},
        {
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": None,
            "add.quadkey._parse_geo_metadata_from_schema": None,
        },
    ),
    "none_metadata": (
        None,
        {
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": None,
            "add.quadkey._parse_geo_metadata_from_schema": None,
        },
    ),
    "empty_geo_value": (
        {b"geo": b""},
        {
            # Reader 1 finds the key and fails the JSON decode; reader 2 treats
            # the falsy value as absent. Same answer today, different route.
            "geo_metadata.parse_geo_metadata": None,
            "crs_utils.parse_geo_metadata_from_schema": None,
            "add.quadkey._parse_geo_metadata_from_schema": None,
        },
    ),
}

DICT_READERS = {
    "geo_metadata.parse_geo_metadata": read_geo_metadata,
    "crs_utils.parse_geo_metadata_from_schema": read_crs_utils,
    "add.quadkey._parse_geo_metadata_from_schema": read_quadkey,
}


@pytest.mark.parametrize("case_name", sorted(EDGE_CASES))
def test_edge_case_reader_divergence(case_name):
    """Pin each dict reader's CURRENT edge-case verdict. Do NOT force equality."""
    metadata, expected = EDGE_CASES[case_name]
    for reader_name, want in expected.items():
        got = DICT_READERS[reader_name](metadata)
        assert got == want, f"{case_name}: {reader_name} returned {got!r}, pinned {want!r}"


def test_str_key_divergence_is_still_present():
    """The headline divergence from #664, asserted directly rather than by table.

    Reader 1 rejects str-keyed metadata; reader 2 parses it. When the
    consolidation lands and reader 1 adopts reader-2 semantics, this test fails
    and is the signal to delete it along with the ``str_key_str_value`` row.
    """
    assert read_geo_metadata(STR_KEY_META) is None
    assert read_crs_utils(STR_KEY_META) == PARSED_STR_KEY


# =============================================================================
# 3. CRS-equality trio
# =============================================================================

# Compact but structurally valid PROJJSON. Each carries an ``id`` member, so the
# helpers take their authority:code fast paths rather than pyproj's semantic
# comparison — that keeps the pinned matrices stable across pyproj versions.
CRS84_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84 (CRS84)",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [{"name": "World Geodetic System 1984 (G2296)"}],
        "accuracy": "2.0",
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
        ],
    },
    "id": {"authority": "OGC", "code": "CRS84"},
}

EPSG4326_PROJJSON = {
    "type": "GeographicCRS",
    "name": "WGS 84",
    "datum_ensemble": {
        "name": "World Geodetic System 1984 ensemble",
        "members": [{"name": "World Geodetic System 1984 (G2296)"}],
        "accuracy": "2.0",
    },
    "coordinate_system": {
        "subtype": "ellipsoidal",
        "axis": [
            {
                "name": "Geodetic latitude",
                "abbreviation": "Lat",
                "direction": "north",
                "unit": "degree",
            },
            {
                "name": "Geodetic longitude",
                "abbreviation": "Lon",
                "direction": "east",
                "unit": "degree",
            },
        ],
    },
    "id": {"authority": "EPSG", "code": 4326},
}

EPSG5070_PROJJSON = {
    "type": "ProjectedCRS",
    "name": "NAD83 / Conus Albers",
    "base_crs": {
        "name": "NAD83",
        "datum": {
            "type": "GeodeticReferenceFrame",
            "name": "North American Datum 1983",
            "ellipsoid": {
                "name": "GRS 1980",
                "semi_major_axis": 6378137,
                "inverse_flattening": 298.257222101,
            },
        },
        "coordinate_system": {
            "subtype": "ellipsoidal",
            "axis": [
                {
                    "name": "Geodetic latitude",
                    "abbreviation": "Lat",
                    "direction": "north",
                    "unit": "degree",
                },
                {
                    "name": "Geodetic longitude",
                    "abbreviation": "Lon",
                    "direction": "east",
                    "unit": "degree",
                },
            ],
        },
        "id": {"authority": "EPSG", "code": 4269},
    },
    "conversion": {
        "name": "Conus Albers",
        "method": {"name": "Albers Equal Area", "id": {"authority": "EPSG", "code": 9822}},
        "parameters": [
            {
                "name": "Latitude of false origin",
                "value": 23,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8821},
            },
            {
                "name": "Longitude of false origin",
                "value": -96,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8822},
            },
            {
                "name": "Latitude of 1st standard parallel",
                "value": 29.5,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8823},
            },
            {
                "name": "Latitude of 2nd standard parallel",
                "value": 45.5,
                "unit": "degree",
                "id": {"authority": "EPSG", "code": 8824},
            },
            {
                "name": "Easting at false origin",
                "value": 0,
                "unit": "metre",
                "id": {"authority": "EPSG", "code": 8826},
            },
            {
                "name": "Northing at false origin",
                "value": 0,
                "unit": "metre",
                "id": {"authority": "EPSG", "code": 8827},
            },
        ],
    },
    "coordinate_system": {
        "subtype": "Cartesian",
        "axis": [
            {"name": "Easting", "abbreviation": "X", "direction": "east", "unit": "metre"},
            {"name": "Northing", "abbreviation": "Y", "direction": "north", "unit": "metre"},
        ],
    },
    "id": {"authority": "EPSG", "code": 5070},
}

# The five matrix inputs, expressed as the geo *column* metadata a caller holds.
# "absent" and "null" are distinct in the file — per the spec an absent ``crs``
# means the OGC:CRS84 default while an explicit ``null`` means an unknown /
# engineering CRS — but both reduce to ``None`` at the helper boundary, because
# every call site extracts the value with ``col_meta.get("crs")``.
# ``test_absent_and_explicit_null_collapse_at_helper_boundary`` pins that
# collapse: it is a genuine information loss the consolidation must decide on.
CRS_COLUMN_META = {
    "absent": {},
    "null": {"crs": None},
    "crs84": {"crs": CRS84_PROJJSON},
    "epsg4326": {"crs": EPSG4326_PROJJSON},
    "epsg5070": {"crs": EPSG5070_PROJJSON},
}
CRS_NAMES = ["absent", "null", "crs84", "epsg4326", "epsg5070"]


def crs_value(name: str):
    """Extract a CRS the way every call site does: ``col_meta.get("crs")``."""
    return CRS_COLUMN_META[name].get("crs")


# Generated by running each helper over the matrix once and hardcoding the
# result (2026-08-22, pyproj as pinned in uv.lock). To regenerate after an
# intentional change, print `{a: {b: fn(crs_value(a), crs_value(b))}}` for each
# helper and paste the result back here — do not relax an assertion to make a
# surprise pass.
IS_CRS84_EQUIVALENT = {
    "absent": True,
    "null": True,
    "crs84": True,
    "epsg4326": True,
    "epsg5070": False,
}

CRS_EQUALS = {
    "absent": {"absent": True, "null": True, "crs84": False, "epsg4326": False, "epsg5070": False},
    "null": {"absent": True, "null": True, "crs84": False, "epsg4326": False, "epsg5070": False},
    "crs84": {"absent": False, "null": False, "crs84": True, "epsg4326": False, "epsg5070": False},
    "epsg4326": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": True,
        "epsg5070": False,
    },
    "epsg5070": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": False,
        "epsg5070": True,
    },
}

CRS_ARE_EQUIVALENT = {
    "absent": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": False,
        "epsg5070": False,
    },
    "null": {"absent": False, "null": False, "crs84": False, "epsg4326": False, "epsg5070": False},
    "crs84": {"absent": False, "null": False, "crs84": True, "epsg4326": False, "epsg5070": False},
    "epsg4326": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": True,
        "epsg5070": False,
    },
    "epsg5070": {
        "absent": False,
        "null": False,
        "crs84": False,
        "epsg4326": False,
        "epsg5070": True,
    },
}

# Cells where the two binary helpers disagree — the consolidation decision list.
# Each entry is (crs_a, crs_b, why it matters). Kept as data so
# ``test_crs_helper_disagreement_inventory_is_exact`` fails if the set of
# disagreements changes in either direction: a newly-agreeing cell means someone
# fixed one silently, a new disagreement means a fresh inconsistency.
CRS_DECISIONS = {
    ("absent", "absent"): (
        "_crs_equals says two missing CRSs are equal (None == None); "
        "_crs_are_equivalent says False because neither side yields an "
        "authority:code and neither is a dict. DECIDE: is 'both default' a match?"
    ),
    ("absent", "null"): (
        "Same collapse as above, and the spec-level distinction (absent => CRS84 "
        "default, null => unknown CRS) is already lost before the helper is called."
    ),
    ("null", "absent"): (
        "Symmetric counterpart of ('absent', 'null'); both helpers are symmetric "
        "here, so the decision is the same one."
    ),
    ("null", "null"): (
        "_crs_equals says two explicitly-unknown CRSs are equal; "
        "_crs_are_equivalent says False. DECIDE: should 'unknown == unknown' hold? "
        "Arguably not — two unknown CRSs are not known to be the same CRS."
    ),
}


@pytest.mark.parametrize("name", CRS_NAMES)
def test_is_crs84_equivalent_matrix(name):
    """Pin ``validate._is_crs84_equivalent`` (unary) over the five CRS values.

    Note this helper takes ONE argument, so it contributes a 5-cell vector to the
    matrix rather than a 5x5 grid.
    """
    assert _is_crs84_equivalent(crs_value(name)) is IS_CRS84_EQUIVALENT[name]


@pytest.mark.parametrize("name_b", CRS_NAMES)
@pytest.mark.parametrize("name_a", CRS_NAMES)
def test_crs_equals_matrix(name_a, name_b):
    """Pin ``validate._crs_equals`` over the full 5x5 matrix."""
    assert _crs_equals(crs_value(name_a), crs_value(name_b)) is CRS_EQUALS[name_a][name_b]


@pytest.mark.parametrize("name_b", CRS_NAMES)
@pytest.mark.parametrize("name_a", CRS_NAMES)
def test_crs_are_equivalent_matrix(name_a, name_b):
    """Pin ``inspect_utils._crs_are_equivalent`` over the full 5x5 matrix.

    Where this disagrees with ``_crs_equals`` the cell is listed in
    ``CRS_DECISIONS``; that disagreement is expected today, not a bug being
    pinned as correct.
    """
    assert (
        _crs_are_equivalent(crs_value(name_a), crs_value(name_b))
        is CRS_ARE_EQUIVALENT[name_a][name_b]
    )


def test_crs_helper_disagreement_inventory_is_exact():
    """The set of cells where the two binary helpers disagree must not drift."""
    observed = {
        (a, b)
        for a in CRS_NAMES
        for b in CRS_NAMES
        if CRS_EQUALS[a][b] is not CRS_ARE_EQUIVALENT[a][b]
    }
    assert observed == set(CRS_DECISIONS), (
        "CRS helper disagreements changed.\n"
        f"  newly agreeing (drop from CRS_DECISIONS): {sorted(set(CRS_DECISIONS) - observed)}\n"
        f"  newly disagreeing (add to CRS_DECISIONS): {sorted(observed - set(CRS_DECISIONS))}"
    )


def test_absent_and_explicit_null_collapse_at_helper_boundary():
    """An absent ``crs`` and an explicit ``"crs": null`` are indistinguishable here.

    The spec gives them different meanings (absent => OGC:CRS84 default;
    null => unknown/engineering CRS), but every call site extracts with
    ``col_meta.get("crs")``, so both arrive as ``None``. Pinned because the
    consolidated CRS reader must decide whether to preserve the distinction.
    """
    assert crs_value("absent") is None
    assert crs_value("null") is None
    for name in CRS_NAMES:
        assert _crs_equals(crs_value("absent"), crs_value(name)) is _crs_equals(
            crs_value("null"), crs_value(name)
        )
