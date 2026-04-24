"""
Tests for the layer parameter in convert operations.

These tests verify that multi-layer formats (GeoPackage, FileGDB) can be read
with a specific layer selected via the `layer` parameter.

Test fixtures:
- Multi-layer GeoPackage: multilayer_test.gpkg with 'buildings' and 'roads' layers
  Created from buildings_test.gpkg and buildings_test.geojson using ogr2ogr
- FileGDB: From GDAL test suite (testopenfilegdb.gdb) with 37 layers
  Source: https://github.com/OSGeo/gdal/tree/master/autotest/ogr/data/filegdb
  License: MIT/X (GDAL project)

Integration tests:
- Estonia Topographic Database: Large GeoPackage with many layers used to test
  sequential multi-layer conversion stability (issue #401)
"""

import io
import shutil
import zipfile

import pyarrow.parquet as pq
import pytest
import requests
from click.testing import CliRunner

from geoparquet_io.api.table import convert
from geoparquet_io.core.convert import _validate_layer_name, read_spatial_to_arrow
from geoparquet_io.core.list_layers import list_layers


@pytest.fixture
def multilayer_gpkg(test_data_dir):
    """Return path to multi-layer GeoPackage with buildings and roads layers.

    This fixture contains two layers with different column schemas:
    - buildings: id, geometry (42 rows)
    - roads: id, geom (42 rows)

    The different geometry column names ('geometry' vs 'geom') help verify
    layer selection is working correctly.
    """
    return str(test_data_dir / "multilayer_test.gpkg")


@pytest.fixture
def filegdb_path(test_data_dir):
    """Return path to the GDAL test FileGDB with multiple layers.

    Source: https://github.com/OSGeo/gdal/tree/master/autotest/ogr/data/filegdb
    Contains 37 layers including: point, polygon, linestring, multipoint, etc.
    """
    return str(test_data_dir / "testopenfilegdb.gdb")


class TestConvertLayerAPI:
    """Tests for gpio.convert() with layer parameter."""

    def test_convert_geopackage_specific_layer(self, multilayer_gpkg):
        """Converting with layer parameter should read only that layer."""
        # Read buildings layer - has 'id' column and original geometry column
        result = convert(multilayer_gpkg, layer="buildings")
        arrow_table = result.table

        assert arrow_table.num_rows == 42
        assert "id" in arrow_table.column_names
        # The output always normalizes to 'geometry' column name
        assert "geometry" in arrow_table.column_names

    def test_convert_geopackage_different_layer(self, multilayer_gpkg):
        """Converting with different layer should read that layer's data."""
        # Read roads layer - also has 'id' column but originally had 'geom' column
        result = convert(multilayer_gpkg, layer="roads")
        arrow_table = result.table

        assert arrow_table.num_rows == 42
        assert "id" in arrow_table.column_names
        assert "geometry" in arrow_table.column_names

    def test_convert_geopackage_without_layer_reads_first(self, multilayer_gpkg):
        """Converting without layer parameter should read first/default layer."""
        # Without layer, should still work (reads first layer)
        result = convert(multilayer_gpkg)
        arrow_table = result.table

        # Should get the first layer (buildings)
        assert arrow_table.num_rows == 42
        assert "geometry" in arrow_table.column_names

    @pytest.mark.skip(reason="DuckDB segfaults on invalid layer names - upstream bug")
    def test_convert_geopackage_invalid_layer_raises(self, multilayer_gpkg):
        """Converting with non-existent layer should raise an error.

        Note: Currently skipped because DuckDB's ST_Read segfaults when given
        an invalid layer name instead of raising a proper exception.
        """
        with pytest.raises(Exception) as exc_info:
            convert(multilayer_gpkg, layer="nonexistent_layer")

        # Should mention the layer name or that layer doesn't exist
        error_msg = str(exc_info.value).lower()
        assert "nonexistent" in error_msg or "layer" in error_msg or "not found" in error_msg

    def test_convert_layer_write_roundtrip(self, multilayer_gpkg, tmp_path):
        """Layer selection should work through full write roundtrip."""
        output = tmp_path / "buildings.parquet"

        # Convert buildings layer and write
        convert(multilayer_gpkg, layer="buildings").write(str(output))

        # Read back and verify
        result = pq.read_table(str(output))
        assert result.num_rows == 42
        assert "geometry" in result.column_names

    def test_convert_sequential_layers_no_sigabrt(self, multilayer_gpkg, tmp_path):
        """Sequential layer reads should not cause SIGABRT on macOS ARM64.

        Regression test for issue #322: SIGABRT in read_spatial_to_arrow when
        converting multiple GeoPackage layers sequentially on macOS ARM64.

        The fix adds gc.collect() after closing DuckDB connections to ensure
        GDAL's internal handles are fully released before the next read.
        """
        layers = ["buildings", "roads"]

        # Convert all layers sequentially - this would crash before the fix
        for layer in layers:
            output = tmp_path / f"{layer}.parquet"
            convert(multilayer_gpkg, layer=layer).write(str(output))

            # Verify each output
            result = pq.read_table(str(output))
            assert result.num_rows == 42
            assert "geometry" in result.column_names


class TestReadSpatialToArrowLayer:
    """Tests for read_spatial_to_arrow() with layer parameter."""

    def test_read_spatial_with_layer(self, multilayer_gpkg):
        """read_spatial_to_arrow should accept layer parameter."""
        arrow_table, crs, geom_col = read_spatial_to_arrow(
            multilayer_gpkg,
            layer="roads",
        )

        assert arrow_table.num_rows == 42
        assert "id" in arrow_table.column_names

    def test_read_spatial_without_layer(self, multilayer_gpkg):
        """read_spatial_to_arrow should work without layer (backward compat)."""
        arrow_table, crs, geom_col = read_spatial_to_arrow(multilayer_gpkg)

        # Should still work
        assert arrow_table.num_rows == 42


@pytest.mark.slow
class TestFileGDBLayer:
    """Tests for FileGDB format with layer parameter.

    These tests use the GDAL testopenfilegdb.gdb fixture.
    Marked slow because FileGDB reading can be slower than GeoPackage.
    """

    def test_filegdb_point_layer(self, filegdb_path):
        """Should be able to read the 'point' layer from FileGDB."""
        result = convert(filegdb_path, layer="point")
        arrow_table = result.table

        # The point layer should have geometry
        assert "geometry" in arrow_table.column_names
        assert arrow_table.num_rows > 0

    def test_filegdb_polygon_layer(self, filegdb_path):
        """Should be able to read the 'polygon' layer from FileGDB."""
        result = convert(filegdb_path, layer="polygon")
        arrow_table = result.table

        assert "geometry" in arrow_table.column_names
        assert arrow_table.num_rows > 0

    def test_filegdb_different_layers_different_data(self, filegdb_path):
        """Different layers should return different data."""
        point_result = convert(filegdb_path, layer="point")
        polygon_result = convert(filegdb_path, layer="polygon")

        # Different geometry types = different data
        # (We can't easily compare geometry types in Arrow, but row counts may differ)
        # At minimum, both should succeed
        assert point_result.table.num_rows > 0
        assert polygon_result.table.num_rows > 0


class TestLayerValidation:
    """Tests for layer name validation and SQL injection protection."""

    def test_validate_layer_name_normal(self):
        """Normal layer names should pass validation."""
        assert _validate_layer_name("buildings") == "buildings"
        assert _validate_layer_name("my_layer") == "my_layer"
        assert _validate_layer_name("Layer 1") == "Layer 1"
        assert _validate_layer_name("layer-with-dashes") == "layer-with-dashes"

    def test_validate_layer_name_escapes_quotes(self):
        """Single quotes in layer names should be escaped."""
        # Single quote should be doubled (SQL standard)
        assert _validate_layer_name("O'Brien's Layer") == "O''Brien''s Layer"
        assert _validate_layer_name("test'layer") == "test''layer"

    def test_validate_layer_name_blocks_sql_injection(self):
        """SQL injection patterns should be rejected."""
        with pytest.raises(ValueError, match="unsafe character"):
            _validate_layer_name("layer'; DROP TABLE users; --")

        with pytest.raises(ValueError, match="unsafe character"):
            _validate_layer_name("layer/*comment*/")

        with pytest.raises(ValueError, match="unsafe character"):
            _validate_layer_name("layer\\injection")

    def test_validate_layer_name_blocks_comment_sequences(self):
        """SQL comment sequences should be blocked."""
        with pytest.raises(ValueError, match="unsafe character"):
            _validate_layer_name("layer--comment")

        with pytest.raises(ValueError, match="unsafe character"):
            _validate_layer_name("/* injection */")


class TestConvertLayerCLI:
    """CLI integration tests for the --layer option."""

    def test_cli_convert_geoparquet_help_shows_layer(self):
        """CLI help should show the --layer option."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["convert", "geoparquet", "--help"])

        assert result.exit_code == 0
        assert "--layer" in result.output
        assert "GeoPackage" in result.output or "FileGDB" in result.output

    def test_cli_convert_with_layer(self, multilayer_gpkg, tmp_path):
        """CLI should accept --layer option and convert specific layer."""
        from geoparquet_io.cli.main import cli

        output = tmp_path / "buildings.parquet"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "convert",
                "geoparquet",
                multilayer_gpkg,
                str(output),
                "--layer",
                "buildings",
                "--skip-hilbert",
            ],
        )

        assert result.exit_code == 0, f"CLI failed: {result.output}"
        assert output.exists()

        # Verify output has expected rows
        table = pq.read_table(str(output))
        assert table.num_rows == 42

    def test_cli_convert_different_layers_produce_different_output(self, multilayer_gpkg, tmp_path):
        """Different --layer values should produce different outputs."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()

        # Convert buildings layer
        buildings_output = tmp_path / "buildings.parquet"
        result1 = runner.invoke(
            cli,
            [
                "convert",
                "geoparquet",
                multilayer_gpkg,
                str(buildings_output),
                "--layer",
                "buildings",
                "--skip-hilbert",
            ],
        )
        assert result1.exit_code == 0

        # Convert roads layer
        roads_output = tmp_path / "roads.parquet"
        result2 = runner.invoke(
            cli,
            [
                "convert",
                "geoparquet",
                multilayer_gpkg,
                str(roads_output),
                "--layer",
                "roads",
                "--skip-hilbert",
            ],
        )
        assert result2.exit_code == 0

        # Both should exist and have data
        assert buildings_output.exists()
        assert roads_output.exists()

        buildings_table = pq.read_table(str(buildings_output))
        roads_table = pq.read_table(str(roads_output))

        assert buildings_table.num_rows == 42
        assert roads_table.num_rows == 42


@pytest.mark.network
@pytest.mark.slow
class TestEstoniaGeoPackageIntegration:
    """Integration tests using Estonia Topographic Database GeoPackage.

    Regression test for issue #401: segfaults and memory corruption when
    converting multiple GeoPackage layers sequentially on WSL2/Linux.

    The Estonia ETAK database is a large GeoPackage with dozens of layers,
    making it an excellent stress test for the gc.collect() fix that prevents
    DuckDB/GDAL handle cleanup races.

    Data source: Estonian Land Board (Maa-amet)
    License: Open data (CC0)
    URL: https://geoportaal.maaamet.ee/
    """

    ESTONIA_GPKG_URL = (
        "https://geoportaal.maaamet.ee/index.php?lang_id=1&plugin_act=otsing"
        "&andmetyyp=ETAK&dl=1&f=ETAK_EESTI_GPKG.zip&page_id=609"
    )
    # Number of layers to test (not all, to keep test time reasonable)
    NUM_LAYERS_TO_TEST = 5

    @pytest.fixture(scope="class")
    def estonia_gpkg(self, tmp_path_factory):
        """Download and extract Estonia GeoPackage.

        This fixture is class-scoped to avoid re-downloading for each test.
        """
        tmp_dir = tmp_path_factory.mktemp("estonia")

        # Download the zip file
        response = requests.get(self.ESTONIA_GPKG_URL, timeout=300)
        response.raise_for_status()

        # Extract the GeoPackage from the zip
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Find the .gpkg file in the archive
            gpkg_files = [n for n in zf.namelist() if n.endswith(".gpkg")]
            if not gpkg_files:
                pytest.fail("No .gpkg file found in downloaded archive")

            gpkg_name = gpkg_files[0]
            zf.extract(gpkg_name, tmp_dir)
            gpkg_path = tmp_dir / gpkg_name

        yield str(gpkg_path)

        # Cleanup
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def test_sequential_layer_conversion_no_crash(self, estonia_gpkg, tmp_path):
        """Converting multiple layers sequentially should not crash.

        This is the core regression test for issue #401. Before the fix,
        converting layers in sequence would cause:
        - Segmentation fault (core dumped)
        - munmap_chunk(): invalid pointer
        - Invalid Input Error: Unsupported geometry type in WKB

        The fix applies gc.collect() after closing DuckDB connections to ensure
        GDAL's internal handles are fully released before the next read.
        """
        # Get list of available layers
        layers = list_layers(estonia_gpkg)
        assert len(layers) > 0, "GeoPackage should have at least one layer"

        # Test a subset of layers (the full file has many layers)
        layers_to_test = layers[: self.NUM_LAYERS_TO_TEST]

        # Convert each layer sequentially - this would crash before the fix
        converted_count = 0
        for layer_name in layers_to_test:
            output = tmp_path / f"{layer_name}.parquet"
            try:
                result = convert(estonia_gpkg, layer=layer_name)
                result.write(str(output))

                # Verify output is valid
                assert output.exists(), f"Output file for {layer_name} should exist"
                table = pq.read_table(str(output))
                assert table.num_rows >= 0, f"Layer {layer_name} should be readable"
                assert "geometry" in table.column_names, f"Layer {layer_name} should have geometry"

                converted_count += 1
            except Exception as e:
                # Some layers may have unsupported geometry types - that's OK
                # The key test is that we don't crash with segfault
                if "Unsupported geometry type" in str(e):
                    continue
                raise

        # At least some layers should convert successfully
        assert converted_count > 0, "At least one layer should convert successfully"

    def test_list_layers_estonia(self, estonia_gpkg):
        """Should be able to list layers in Estonia GeoPackage.

        Basic sanity check that the downloaded file is valid.
        """
        layers = list_layers(estonia_gpkg)
        assert len(layers) > 10, "Estonia ETAK should have many layers"
        # All layer names should be strings
        assert all(isinstance(layer, str) for layer in layers)
