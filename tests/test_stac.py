"""
Tests for core/stac.py: STAC Item and Collection generation from local files.

Everything here is offline — STAC generation deliberately refuses remote
inputs, so the whole module is exercisable against the small committed
fixtures.
"""

import json
import shutil
from pathlib import Path

import pytest

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.stac import (
    construct_asset_href,
    detect_pmtiles,
    detect_stac,
    generate_item_id,
    generate_stac_collection,
    generate_stac_geometry,
    generate_stac_item,
    get_file_datetime,
    write_stac_json,
)

_DATA = Path(__file__).parent / "data"
PLACES = str(_DATA / "places_test.parquet")
BUILDINGS = str(_DATA / "buildings_test.parquet")
# GeoParquet 2.0 fixture whose geo metadata carries a PROJJSON CRS
# (EPSG:5070), which is what feeds the proj:* item properties.
GPQ2_WITH_CRS = str(_DATA / "fields_gpq2_5070_brotli.parquet")


class TestDetectStac:
    """detect_stac distinguishes Items, Collections, and everything else."""

    def test_item_json(self, tmp_path):
        p = tmp_path / "item.json"
        p.write_text(json.dumps({"type": "Feature", "id": "x"}))
        assert detect_stac(str(p)) == "Item"

    def test_collection_json(self, tmp_path):
        p = tmp_path / "collection.json"
        p.write_text(json.dumps({"type": "Collection", "id": "x"}))
        assert detect_stac(str(p)) == "Collection"

    def test_non_json_file(self):
        assert detect_stac(PLACES) is None

    def test_invalid_json(self, tmp_path):
        p = tmp_path / "broken.json"
        p.write_text("{not json")
        assert detect_stac(str(p)) is None

    def test_json_without_stac_type(self, tmp_path):
        p = tmp_path / "other.json"
        p.write_text(json.dumps({"type": "FeatureCollection"}))
        assert detect_stac(str(p)) is None

    def test_pure_stac_directory(self, tmp_path):
        (tmp_path / "collection.json").write_text(json.dumps({"type": "Collection"}))
        assert detect_stac(str(tmp_path)) == "Collection"

    def test_mixed_directory_is_not_stac(self, tmp_path):
        """A directory with collection.json AND parquet files is 'mixed'."""
        (tmp_path / "collection.json").write_text(json.dumps({"type": "Collection"}))
        shutil.copy(PLACES, tmp_path / "data.parquet")
        assert detect_stac(str(tmp_path)) is None

    def test_directory_without_collection(self, tmp_path):
        assert detect_stac(str(tmp_path)) is None

    def test_nonexistent_path(self):
        assert detect_stac("does/not/exist") is None


class TestDetectPmtiles:
    """detect_pmtiles: none -> None, one -> path, many -> error."""

    def test_no_pmtiles(self, tmp_path):
        assert detect_pmtiles(str(tmp_path)) is None

    def test_single_pmtiles(self, tmp_path):
        p = tmp_path / "overview.pmtiles"
        p.write_bytes(b"")
        assert detect_pmtiles(str(tmp_path), verbose=True) == str(p)

    def test_single_pmtiles_next_to_file(self, tmp_path):
        """For a file input, the file's directory is searched."""
        parquet = tmp_path / "data.parquet"
        shutil.copy(PLACES, parquet)
        p = tmp_path / "overview.pmtiles"
        p.write_bytes(b"")
        assert detect_pmtiles(str(parquet)) == str(p)

    def test_multiple_pmtiles_raises(self, tmp_path):
        (tmp_path / "a.pmtiles").write_bytes(b"")
        (tmp_path / "b.pmtiles").write_bytes(b"")
        with pytest.raises(InvalidParameterError, match="Multiple PMTiles files"):
            detect_pmtiles(str(tmp_path))


class TestSmallHelpers:
    def test_generate_item_id_from_filename(self):
        assert generate_item_id("some/dir/usa.parquet") == "usa"

    def test_generate_item_id_from_partition_key(self):
        assert generate_item_id("some/dir/usa.parquet", partition_key="can") == "can"

    def test_construct_asset_href_bucket(self):
        assert (
            construct_asset_href("usa.parquet", "s3://bucket/path/")
            == "s3://bucket/path/usa.parquet"
        )

    def test_construct_asset_href_public_url_wins(self):
        assert (
            construct_asset_href("usa.parquet", "s3://bucket/path", "https://cdn.example.com/")
            == "https://cdn.example.com/usa.parquet"
        )

    def test_get_file_datetime_uses_mtime(self):
        dt = get_file_datetime(PLACES)
        assert dt.tzinfo is not None

    def test_get_file_datetime_missing_file_falls_back_to_now(self):
        dt = get_file_datetime("does/not/exist.parquet")
        assert dt.tzinfo is not None

    def test_generate_stac_geometry_is_closed_polygon(self):
        geom = generate_stac_geometry(PLACES)
        assert geom["type"] == "Polygon"
        ring = geom["coordinates"][0]
        assert len(ring) == 5
        assert ring[0] == ring[-1]
        # The places fixture covers northern Ghana/Togo.
        xmin, ymin = ring[0]
        assert -2 < xmin < 0
        assert 9 < ymin < 10


class TestGenerateStacItem:
    def test_item_structure(self):
        item = generate_stac_item(PLACES, "s3://bucket/places/")
        assert item["type"] == "Feature"
        assert item["id"] == "places_test"
        # bbox matches the fixture's extent.
        xmin, ymin, xmax, ymax = item["bbox"]
        assert xmin < xmax and ymin < ymax
        assert -2 < xmin < 0 and 9 < ymin < 10
        # The data asset points into the bucket.
        data = item["assets"]["data"]
        assert data["href"] == "s3://bucket/places/places_test.parquet"
        assert data["type"] == "application/vnd.apache.parquet"
        assert data["roles"] == ["data"]
        # Self link present.
        self_links = [link for link in item["links"] if link["rel"] == "self"]
        assert self_links and self_links[0]["href"] == "s3://bucket/places/places_test.json"

    def test_item_with_public_url_and_custom_id(self):
        item = generate_stac_item(
            PLACES, "s3://bucket/places", public_url="https://data.example.com", item_id="ghana"
        )
        assert item["id"] == "ghana"
        assert item["assets"]["data"]["href"] == "https://data.example.com/places_test.parquet"

    def test_item_projection_properties_from_projjson_crs(self):
        item = generate_stac_item(GPQ2_WITH_CRS, "s3://bucket/x/", verbose=True)
        assert item["properties"]["proj:epsg"] == 5070
        assert item["properties"]["proj:projjson"]["id"] == {"authority": "EPSG", "code": 5070}

    def test_item_includes_pmtiles_overview(self, tmp_path):
        parquet = tmp_path / "data.parquet"
        shutil.copy(PLACES, parquet)
        (tmp_path / "overview.pmtiles").write_bytes(b"")
        item = generate_stac_item(str(parquet), "s3://bucket/x")
        overview = item["assets"]["overview"]
        assert overview["href"] == "s3://bucket/x/overview.pmtiles"
        assert overview["type"] == "application/vnd.pmtiles"
        assert set(overview["roles"]) == {"visual", "overview"}

    def test_remote_input_refused(self):
        with pytest.raises(InvalidParameterError, match="requires local parquet files"):
            generate_stac_item("s3://bucket/remote.parquet", "s3://bucket/")


class TestGenerateStacCollection:
    @pytest.fixture
    def partition_dir(self, tmp_path):
        """A two-partition dataset made from the committed fixtures."""
        d = tmp_path / "parts"
        d.mkdir()
        shutil.copy(PLACES, d / "places.parquet")
        shutil.copy(BUILDINGS, d / "buildings.parquet")
        return d

    def test_collection_structure(self, partition_dir):
        collection, items = generate_stac_collection(
            str(partition_dir), "s3://bucket/parts/", verbose=True
        )
        assert collection["type"] == "Collection"
        assert collection["id"] == "parts"
        assert len(items) == 2
        assert {i["id"] for i in items} == {"places", "buildings"}
        # Every item links back to the collection.
        for item in items:
            rels = {link["rel"] for link in item["links"]}
            assert "collection" in rels
        # The collection links to each item, plus itself.
        item_links = [link for link in collection["links"] if link["rel"] == "item"]
        assert len(item_links) == 2
        # Overall extent is the union of both item bboxes.
        overall = collection["extent"]["spatial"]["bbox"][0]
        for item in items:
            xmin, ymin, xmax, ymax = item["bbox"]
            assert overall[0] <= xmin and overall[1] <= ymin
            assert overall[2] >= xmax and overall[3] >= ymax

    def test_collection_picks_up_pmtiles(self, partition_dir):
        (partition_dir / "overview.pmtiles").write_bytes(b"")
        collection, _ = generate_stac_collection(str(partition_dir), "s3://bucket/parts")
        overview = collection["assets"]["overview"]
        assert overview["href"] == "s3://bucket/parts/overview.pmtiles"

    def test_collection_finds_hive_partitions(self, tmp_path):
        d = tmp_path / "hive"
        (d / "key=a").mkdir(parents=True)
        shutil.copy(PLACES, d / "key=a" / "places.parquet")
        collection, items = generate_stac_collection(str(d), "s3://bucket/hive")
        assert len(items) == 1
        assert items[0]["id"] == "places"

    def test_empty_directory_raises(self, tmp_path):
        with pytest.raises(FileNotFoundGeoParquetError):
            generate_stac_collection(str(tmp_path), "s3://bucket/x")

    def test_remote_input_refused(self):
        with pytest.raises(InvalidParameterError, match="requires a local directory"):
            generate_stac_collection("s3://bucket/parts/", "s3://bucket/parts/")


class TestWriteStacJson:
    def test_round_trip(self, tmp_path):
        out = tmp_path / "item.json"
        write_stac_json({"type": "Feature", "id": "x"}, str(out), verbose=True)
        assert json.loads(out.read_text()) == {"type": "Feature", "id": "x"}
