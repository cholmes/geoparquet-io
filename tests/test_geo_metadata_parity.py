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

Divergence versus *spec violation*. A cell where the helpers merely disagree
with each other is pinned green — the consolidation gets to pick a winner. A
cell where the current answer contradicts the GeoParquet specification is NOT
pinned green: it is an ``xfail(strict=True)`` asserting the spec-correct
verdict, with the governing rule quoted in the reason and the follow-up issue
referenced. There are three such cells today, all tracked by #699 ("CRS helpers
can't distinguish absent ``crs`` from explicit ``crs: null``, and resolve the
ambiguity in opposite directions"). ``strict=True`` is load-bearing: when #699
is fixed these turn into XPASS and fail the run, which is the signal to delete
the marks rather than let a fix land unnoticed.
"""

import json
from itertools import product
from pathlib import Path
from unittest import mock

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add import quadkey as quadkey_module
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
from geoparquet_io.core.validate import (
    CheckStatus,
    _check_crs_valid,
    _crs_equals,
    _is_crs84_equivalent,
    _version_at_least,
)

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


def _assert_geo_key_sources_agree(path: Path) -> None:
    """Guard: the harness's two metadata *sources* must carry the same ``geo``.

    This suite feeds readers 1, 2 and 5 from ``schema_arrow.metadata`` while
    readers 3 and 4 go to the parquet footer key-value block themselves. Those
    are not the same dict. When a file carries an ``ARROW:schema`` footer key,
    pyarrow rebuilds ``schema_arrow.metadata`` by deserializing that key rather
    than by reading the footer kv, so a footer entry written *after*
    ``ARROW:schema`` is invisible to ``schema_arrow.metadata``. Most local
    fixtures carry ``ARROW:schema``.

    Without this guard a mismatch between the two sources would surface as a
    "readers disagree" failure and send the reader implementations, not the
    file, to the debugger. It also protects ``_has_geo_key`` (which asks only
    ``schema_arrow.metadata``): a file whose ``geo`` lives solely in the footer
    would be misfiled into ``NON_GEO_FIXTURES`` and fail there with an equally
    misleading message.
    """
    pf = pq.ParquetFile(path)
    from_schema = (pf.schema_arrow.metadata or {}).get(b"geo")
    from_footer = (pf.metadata.metadata or {}).get(b"geo")
    assert from_schema == from_footer, (
        f"{path.name}: the two metadata sources this harness uses disagree, so any "
        f"reader disagreement below would be a harness artifact, not a reader bug. "
        f"schema_arrow.metadata[geo]={from_schema!r} "
        f"footer kv[geo]={from_footer!r} "
        f"(ARROW:schema present: {b'ARROW:schema' in (pf.metadata.metadata or {})})"
    )


def _assert_readers_agree(path: Path) -> None:
    """Every reader must produce the same parsed ``geo`` dict for ``path``."""
    _assert_geo_key_sources_agree(path)
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
    """Whether ``path`` carries a ``geo`` kv key. Runs at COLLECTION time.

    An unreadable/corrupt parquet is treated as non-geo rather than allowed to
    raise: a raise here happens during collection and would take down the whole
    module. Routing it to NON_GEO_FIXTURES instead means the bad file surfaces as
    one failing case in ``test_readers_agree_on_non_geo_fixtures`` (where a reader
    will raise on it), which is a far more useful signal than a collection error.
    """
    try:
        metadata = _schema_metadata(path)
    except Exception:
        return False
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

# Same guard for the non-geo half (7 today). Without it, a change that made
# _has_geo_key wrongly report True for everything would empty this list and
# silently skip the whole absent-key test rather than failing.
MIN_NON_GEO_FIXTURES = 5


def test_geo_fixture_inventory_is_populated():
    """Guard: a vanished fixture set must fail loudly, not silently skip."""
    assert len(GEO_FIXTURES) >= MIN_GEO_FIXTURES, (
        f"expected >= {MIN_GEO_FIXTURES} geo fixtures under {DATA}, "
        f"found {len(GEO_FIXTURES)}: {[p.name for p in GEO_FIXTURES]}"
    )


def test_non_geo_fixture_inventory_is_populated():
    """Guard: the absent-geo-key cases must not silently vanish either."""
    assert len(NON_GEO_FIXTURES) >= MIN_NON_GEO_FIXTURES, (
        f"expected >= {MIN_NON_GEO_FIXTURES} non-geo fixtures under {DATA}, "
        f"found {len(NON_GEO_FIXTURES)}: {[p.name for p in NON_GEO_FIXTURES]}"
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
    # Run first: if the file actually has a footer ``geo`` that
    # ``schema_arrow.metadata`` hides, it was misfiled into this list and the
    # source guard says so instead of "reader X returned non-None".
    _assert_geo_key_sources_agree(fixture)
    metadata = _schema_metadata(fixture)
    for name, reader in READERS.items():
        assert reader(fixture, metadata) is None, f"{name} returned non-None for {fixture.name}"


@pytest.mark.corpus
@pytest.mark.integration
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

    Asserting ``read_quadkey(md) == read_crs_utils(md)`` would be ``f(x) == f(x)``
    while the delegation holds — it can never fail, so it would not detect the
    re-divergence it claims to guard. Patching the name that reader 5 delegates
    *through* does: if someone reimplements the parse inline, the patched
    delegate is not called and this fails.
    """
    assert read_quadkey.__module__ == "geoparquet_io.core.add.quadkey"
    metadata = {"geo": '{"version": "1.1.0", "columns": {}}'}
    sentinel = {"version": "sentinel-not-a-real-parse"}

    # patch.object, never a dotted string target: geoparquet_io/__init__.py
    # rebinds geoparquet_io.cli to a Click Group, and dotted-string patching
    # under that package breaks on Python 3.10 only. Use the idiom everywhere.
    with mock.patch.object(
        quadkey_module, "parse_geo_metadata_from_schema", return_value=sentinel
    ) as delegate:
        result = read_quadkey(metadata)

    delegate.assert_called_once_with(metadata)
    assert result is sentinel, "reader 5 must return reader 2's result unmodified"


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


# IMPORTANT — "absent" and "null" are the SAME CALL in both binary matrices and
# in the unary vector. ``crs_value`` returns None for each, and the helpers take
# CRS *values*, not column-metadata dicts, so no helper can tell them apart.
# Every ("absent", x) cell below is therefore byte-identical to the ("null", x)
# cell that follows it; they are kept as two rows because the *spec* gives them
# different required answers, and holding both rows is what makes the conflict
# visible. Where the two rows demand opposite verdicts (see the xfail cells) the
# helper signature cannot satisfy both — that is precisely the defect #699
# tracks, and fixing it means giving the helpers a way to distinguish the two.
#
# Generated by running each helper over the matrix once and hardcoding the
# result (2026-08-22, pyproj as pinned in uv.lock), EXCEPT for the cells listed
# in SPEC_XFAIL_* below, which hold the spec-correct answer that production does
# not yet give. To regenerate after an intentional change, print
# `{a: {b: fn(crs_value(a), crs_value(b))}}` for each helper and paste the
# result back here — do not relax an assertion to make a surprise pass, and do
# not overwrite an xfail cell with the observed value.

# Cells asserting the spec-correct verdict that production currently gets wrong.
# Each is run as xfail(strict=True) so that fixing #699 reports XPASS and fails.
SPEC_XFAIL_IS_CRS84 = {
    "null": (
        "#699 — spec rule: an omitted `crs` defaults to OGC:CRS84, while a `crs` "
        "explicitly present as null means the CRS is unknown/undefined. So an "
        "explicit null is NOT CRS84, and _is_crs84_equivalent(None) returning True "
        "for it is wrong. It returns True only because the value-level signature "
        "already collapsed absent and null into the same None. gpio already shipped "
        "this distinction elsewhere (root CHANGELOG, `Distinguish an explicit "
        "crs: null (CRS *unknown*) from an omitted crs key`, #471, and "
        "`gpio convert reproject --assume-crs84`), so this is a violation, not an "
        "open design question."
    ),
}

SPEC_XFAIL_CRS_EQUALS = {
    ("absent", "crs84"): (
        "#699 — spec rule: an omitted `crs` defaults to OGC:CRS84, so an absent CRS "
        "and an explicit OGC:CRS84 are the same CRS and must compare equal. "
        "_crs_equals never resolves the default: `crs1 is None or crs2 is None` "
        "returns False before any comparison happens."
    ),
    ("crs84", "epsg4326"): (
        "#699 — spec rule: GeoParquet fixes the stored coordinate order to (x, y) "
        "regardless of the CRS's own axis definition, so OGC:CRS84 and EPSG:4326 "
        "describe the same coordinates and must compare equal. _crs_equals "
        "documents exactly that intent (it passes `ignore_axis_order=True`, and its "
        "own comment reads 'Axis order is ignored because GeoParquet fixes "
        "coordinate order to (x, y)') but the `id1 and id2 -> id1 == id2` fast path "
        "returns first and defeats it for the single most common equivalent pair. "
        "inspect_utils._crs_are_equivalent has the identical fast-path defect."
    ),
}

IS_CRS84_EQUIVALENT = {
    "absent": True,
    "null": False,  # SPEC value, xfail: production returns True
    "crs84": True,
    "epsg4326": True,
    "epsg5070": False,
}

# NOTE on the mirror cells. ("crs84", "absent") and ("epsg4326", "crs84") are the
# same two bugs seen from the other side; _crs_equals is symmetric, so they are
# equally wrong. They stay pinned at the observed False so that #699 gets exactly
# one xfail per distinct violation rather than one per ordered pair. When #699
# lands, the two xfails XPASS *and* these two mirrors fail — both signals point
# at the same edit.
CRS_EQUALS = {
    "absent": {"absent": True, "null": True, "crs84": True, "epsg4326": False, "epsg5070": False},
    "null": {"absent": True, "null": True, "crs84": False, "epsg4326": False, "epsg5070": False},
    "crs84": {"absent": False, "null": False, "crs84": True, "epsg4326": True, "epsg5070": False},
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
# fixed one silently, a new disagreement means a fresh inconsistency. That guard
# recomputes the observed set by CALLING both helpers — it must never derive it
# from the pinned matrices above, or it degrades into a tautology over constants.
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

# The trio's LARGEST split is not between the two binary helpers — it is between
# the unary helper and both binaries. ``_is_crs84_equivalent(x)`` asks "is x the
# CRS84 default?" while ``_crs_equals(x, crs84)`` / ``_crs_are_equivalent(x, crs84)``
# ask "is x equal to the CRS84 value?" Those should be the same question, and for
# three of five inputs they give opposite answers (unary True, both binaries
# False). ``test_unary_vs_binary_disagreement_inventory_is_exact`` recomputes this
# set by CALLING the helpers, so this dict is documentation of the observed split,
# never the source the guard checks itself against.
UNARY_DECISIONS = {
    "absent": (
        "_is_crs84_equivalent(None) is True (absent CRS defaults to OGC:CRS84 per "
        "spec), but comparing the same absent value against the CRS84 PROJJSON is "
        "False in BOTH binaries. DECIDE: should the binaries resolve the default "
        "before comparing, so absent == CRS84?"
    ),
    "null": (
        "_is_crs84_equivalent(None) is True, but an explicit null means UNKNOWN CRS, "
        "not CRS84 — the unary is arguably wrong here and only reaches True because "
        "the null/absent collapse (see below) already erased the distinction. "
        "DECIDE: this looks like a latent bug, not merely an inconsistency."
    ),
    "epsg4326": (
        "_is_crs84_equivalent(EPSG:4326) is True — GeoParquet fixes axis order to "
        "(x, y), so EPSG:4326 metadata describes the same coordinates as OGC:CRS84. "
        "Both binaries say EPSG:4326 != OGC:CRS84 because they trust differing "
        "authority:code ids. DECIDE: should the binaries adopt the unary's "
        "axis-order-agnostic CRS84/4326 equivalence?"
    ),
}

# The two binary helpers also disagree on what pyproj equality MEANS when they
# fall back to semantic comparison:
#
#   validate._crs_equals (~:2735):
#       PyprojCRS.from_json_dict(crs1).equals(..., ignore_axis_order=True)
#   inspect_utils._crs_are_equivalent (~:155):
#       CRS.from_json_dict(crs1).equals(CRS.from_json_dict(crs2))   # default: axis order MATTERS
#
# The 5x5 matrix cannot reach this: every value in it carries a complete ``id``,
# so both helpers short-circuit on the fast path and pyproj is never called at
# all. ``test_id_less_projjson_axis_order_split_is_exact`` covers it with id-less
# PROJJSON, pinning only that the two helpers DISAGREE rather than either
# absolute verdict, so it does not churn with the pyproj/PROJ version.


def _param_id_pair(name_a: str, name_b: str) -> str:
    return f"{name_a}-{name_b}"


# Unmarked product, for the helper whose cells are all pinned as observed.
CRS_PAIRS = [
    pytest.param(name_a, name_b, id=_param_id_pair(name_a, name_b))
    for name_a, name_b in product(CRS_NAMES, repeat=2)
]

# Same product, with the two _crs_equals spec violations marked xfail.
CRS_EQUALS_PAIRS = [
    pytest.param(
        name_a,
        name_b,
        id=_param_id_pair(name_a, name_b),
        marks=(
            [
                pytest.mark.xfail(
                    strict=True,
                    raises=AssertionError,
                    reason=SPEC_XFAIL_CRS_EQUALS[(name_a, name_b)],
                )
            ]
            if (name_a, name_b) in SPEC_XFAIL_CRS_EQUALS
            else []
        ),
    )
    for name_a, name_b in product(CRS_NAMES, repeat=2)
]

CRS_UNARY_PARAMS = [
    pytest.param(
        name,
        id=name,
        marks=(
            [
                pytest.mark.xfail(
                    strict=True,
                    raises=AssertionError,
                    reason=SPEC_XFAIL_IS_CRS84[name],
                )
            ]
            if name in SPEC_XFAIL_IS_CRS84
            else []
        ),
    )
    for name in CRS_NAMES
]


@pytest.mark.parametrize("name", CRS_UNARY_PARAMS)
def test_is_crs84_equivalent_matrix(name):
    """Pin ``validate._is_crs84_equivalent`` (unary) over the five CRS values.

    Note this helper takes ONE argument, so it contributes a 5-cell vector to the
    matrix rather than a 5x5 grid. The "absent" and "null" cells make the very
    same call, ``_is_crs84_equivalent(None)``, and demand opposite answers — the
    "null" cell is the xfail, and its existence next to a passing "absent" cell
    is the clearest statement of what #699 has to change.
    """
    assert _is_crs84_equivalent(crs_value(name)) is IS_CRS84_EQUIVALENT[name]


@pytest.mark.parametrize(("name_a", "name_b"), CRS_EQUALS_PAIRS)
def test_crs_equals_matrix(name_a, name_b):
    """Pin ``validate._crs_equals`` over the full 5x5 matrix.

    Rows "absent" and "null" issue identical calls (both reduce to None); they
    are held separately because the spec requires different answers of them.
    """
    assert _crs_equals(crs_value(name_a), crs_value(name_b)) is CRS_EQUALS[name_a][name_b]


@pytest.mark.parametrize(("name_a", "name_b"), CRS_PAIRS)
def test_crs_are_equivalent_matrix(name_a, name_b):
    """Pin ``inspect_utils._crs_are_equivalent`` over the full 5x5 matrix.

    Where this disagrees with ``_crs_equals`` the cell is listed in
    ``CRS_DECISIONS``; that disagreement is expected today, not a bug being
    pinned as correct.

    No xfail marks here even though this helper shares both #699 defects
    (absent != CRS84, CRS84 != EPSG:4326): the xfails live on the ``_crs_equals``
    matrix so #699 carries one xfail per distinct violation rather than one per
    helper. Fixing #699 turns those into XPASS and makes the mirror cells here
    fail — the same edit resolves both signals.
    """
    assert (
        _crs_are_equivalent(crs_value(name_a), crs_value(name_b))
        is CRS_ARE_EQUIVALENT[name_a][name_b]
    )


def test_crs_helper_disagreement_inventory_is_exact():
    """The set of cells where the two binary helpers disagree must not drift.

    ``observed`` is built by CALLING both helpers, never by reading the pinned
    matrices. Deriving it from the literals would make this a tautology over
    constants that stays green through any production change — the exact
    failure mode it exists to catch.

    Scope: this covers the id fast path only, because every value in the matrix
    carries a complete ``id``. The pyproj-fallback half of the split is pinned
    by ``test_id_less_projjson_axis_order_split_is_exact``.
    """
    observed = {
        (a, b)
        for a, b in product(CRS_NAMES, repeat=2)
        if _crs_equals(crs_value(a), crs_value(b))
        is not _crs_are_equivalent(crs_value(a), crs_value(b))
    }
    assert observed == set(CRS_DECISIONS), (
        "CRS helper disagreements changed.\n"
        f"  newly agreeing (drop from CRS_DECISIONS): {sorted(set(CRS_DECISIONS) - observed)}\n"
        f"  newly disagreeing (add to CRS_DECISIONS): {sorted(observed - set(CRS_DECISIONS))}"
    )


def test_unary_vs_binary_disagreement_inventory_is_exact():
    """ "Is x CRS84?" and "does x equal CRS84?" must not silently start agreeing.

    Guards the unary-vs-binary split the same way the binary-vs-binary guard
    does, and with the same rule: ``observed`` comes from CALLING the three
    helpers, so consolidating the trio forces an explicit update to
    UNARY_DECISIONS instead of passing silently over stale literals.
    """
    crs84 = crs_value("crs84")
    observed = {
        name
        for name in CRS_NAMES
        if _is_crs84_equivalent(crs_value(name)) is not _crs_equals(crs_value(name), crs84)
        or _is_crs84_equivalent(crs_value(name)) is not _crs_are_equivalent(crs_value(name), crs84)
    }
    assert observed == set(UNARY_DECISIONS), (
        "unary-vs-binary CRS84 disagreements changed.\n"
        f"  newly agreeing (drop from UNARY_DECISIONS): {sorted(set(UNARY_DECISIONS) - observed)}\n"
        f"  newly disagreeing (add to UNARY_DECISIONS): {sorted(observed - set(UNARY_DECISIONS))}"
    )


def test_absent_and_explicit_null_collapse_at_helper_boundary():
    """An absent ``crs`` and an explicit ``"crs": null`` are indistinguishable here.

    The spec gives them different meanings (absent => OGC:CRS84 default;
    null => unknown/engineering CRS). ``validate._check_crs_valid`` DOES preserve
    the distinction, and it does so by testing membership *before* extracting::

        if "crs" not in col_meta:      # validate.py:307 -> defaults to OGC:CRS84
            ...
        crs = col_meta.get("crs")      # validate.py:315 -> None for BOTH shapes

    Every CRS *equality* call site uses only the second idiom, so the distinction
    is already gone by the time the trio is called.

    The two halves below carry different weight. The ``_check_crs_valid``
    PASSED-vs-WARNING half is the real pin: it exercises production code and
    fails if an explicit null ever collapses into the absent branch. The
    ``.get("crs") is None`` half documents the collapse at the extraction idiom
    itself — it is a property of ``dict.get`` on these two shapes, so it stays
    green under any production change and is here to make the mechanism explicit
    at the point of use, not to detect a regression.
    """
    absent_col = CRS_COLUMN_META["absent"]
    null_col = CRS_COLUMN_META["null"]

    # Production path that DOES distinguish them: different check outcomes.
    assert _check_crs_valid(absent_col, "geometry").status is CheckStatus.PASSED
    assert _check_crs_valid(null_col, "geometry").status is CheckStatus.WARNING

    # The extraction the equality helpers actually receive: identical for both.
    assert "crs" not in absent_col and "crs" in null_col, (
        "fixture shapes must differ, otherwise the collapse below is trivially true"
    )
    assert absent_col.get("crs") is None
    assert null_col.get("crs") is None


def test_id_less_projjson_axis_order_split_is_exact():
    """The binaries' pyproj fallbacks disagree about axis order. Pin the split.

    This is the only material behavioral difference between the two binary
    helpers that the 5x5 matrix cannot reach: every matrix value carries a
    complete ``id``, so both helpers return from their fast path and pyproj is
    never called. Strip the ``id`` and the fallbacks run, revealing::

        validate._crs_equals             -> .equals(..., ignore_axis_order=True)
        inspect_utils._crs_are_equivalent -> .equals(...)   # axis order MATTERS

    What is pinned is only that the two helpers DISAGREE on this pair, not
    either absolute verdict: the absolute answers come out of PROJ and would
    make this test churn with the pyproj/PROJ version, whereas the disagreement
    is a property of the two call sites. When the consolidation picks one
    fallback contract, this test fails and should be deleted.

    The PROJJSON is generated by pyproj rather than hand-written because a
    hand-trimmed datum ensemble is not parseable ("ensemble should have at least
    2 datums"), which would send BOTH helpers down their ``except -> False``
    path and hide the split behind spurious agreement.
    """
    from pyproj import CRS as PyprojCRS

    lon_lat = PyprojCRS.from_user_input("OGC:CRS84").to_json_dict()
    lat_lon = PyprojCRS.from_epsg(4326).to_json_dict()
    for projjson in (lon_lat, lat_lon):
        projjson.pop("id", None)

    # Preconditions, or the assertion below could pass through the fast path.
    assert "id" not in lon_lat and "id" not in lat_lon
    assert [axis["abbreviation"] for axis in lon_lat["coordinate_system"]["axis"]] == ["Lon", "Lat"]
    assert [axis["abbreviation"] for axis in lat_lon["coordinate_system"]["axis"]] == ["Lat", "Lon"]
    # Both must be pyproj-parseable, else both helpers fail closed and "agree".
    assert PyprojCRS.from_json_dict(lon_lat) and PyprojCRS.from_json_dict(lat_lon)

    assert _crs_equals(lon_lat, lat_lon) is not _crs_are_equivalent(lon_lat, lat_lon), (
        "the two binary helpers now agree on an id-less lon/lat vs lat/lon pair. "
        "Either the fallback contracts were unified (delete this test and record "
        "the winner) or a pyproj change made .equals() axis-order-agnostic."
    )


# =============================================================================
# 4. Version gating
# =============================================================================


@pytest.mark.parametrize(
    ("version", "major", "minor", "expected"),
    [
        # The boundary itself: >= must include the equal case. An off-by-one
        # (">=" turned into ">") is invisible to every other test in the repo.
        ("1.1.0", 1, 1, True),
        ("1.1", 1, 1, True),
        ("2.0.0", 2, 0, True),
        ("1.0.0", 1, 0, True),
        # Just below the boundary.
        ("1.0.0", 1, 1, False),
        ("1.9.0", 2, 0, False),
        # Parsed numerically, not lexicographically: "1.10.0" > "1.2.0".
        ("1.10.0", 1, 2, True),
        ("1.2.0", 1, 10, False),
        # Missing minor defaults to 0; pre-release/build tags are stripped.
        ("2", 2, 0, True),
        ("1.0.0-beta.1", 1, 0, True),
        ("1.1.0-beta.1", 1, 1, True),
        ("1.0.0+build.5", 1, 0, True),
        # Unparsable or non-string input fails closed.
        ("not-a-version", 1, 0, False),
        ("", 1, 0, False),
        (None, 1, 0, False),
        (1.1, 1, 1, False),
    ],
)
def test_version_at_least_boundary(version, major, minor, expected):
    """Pin ``validate._version_at_least``, which gates version-specific geo checks.

    It decides which ``geo`` metadata rules apply (covering requires >= 1.1,
    the 2.0 branch changes the encoding vocabulary), so an off-by-one here
    silently applies the wrong spec version to a file's metadata.
    """
    assert _version_at_least(version, major, minor) is expected
