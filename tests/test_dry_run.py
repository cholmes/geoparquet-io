"""
Tests for dry-run functionality in add commands.
"""

from click.testing import CliRunner

from geoparquet_io.cli.main import add


class TestDryRunCommands:
    """Test suite for dry-run functionality."""

    def test_add_bbox_dry_run(self, buildings_test_file):
        """Test dry-run mode for add bbox command."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, "output.parquet", "--dry-run"])

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "COPY (" in result.output
        assert "STRUCT_PACK(" in result.output
        assert "ST_XMin" in result.output
        assert "ST_YMin" in result.output
        assert "ST_XMax" in result.output
        assert "ST_YMax" in result.output
        assert "FORMAT PARQUET" in result.output
        # Should show geometry column name
        assert "-- Geometry column:" in result.output
        # Should not actually create the file
        assert "Successfully added" not in result.output

    def test_add_bbox_dry_run_with_custom_name(self, buildings_test_file):
        """Test dry-run mode with custom bbox column name."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["bbox", buildings_test_file, "output.parquet", "--bbox-name", "bounds", "--dry-run"],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert 'AS "bounds"' in result.output
        assert "-- New column: bounds" in result.output

    def test_add_admin_divisions_dry_run(self, buildings_test_file):
        """Test dry-run mode for add admin-divisions command."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["admin-divisions", buildings_test_file, "output.parquet", "--dry-run", "--no-cache"],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Should show admin dataset info
        assert "Admin dataset:" in result.output
        assert "s3://nlebovits/gaul-l2-admin" in result.output
        # Should show spatial join query
        assert "ST_Intersects" in result.output
        # New default: dataset-prefixed columns
        assert "gaul_continent" in result.output or "gaul_country" in result.output
        # Should show COPY statement
        assert "COPY (" in result.output
        assert "TO 'output.parquet'" in result.output

    def test_add_admin_divisions_dry_run_with_specific_levels(self, buildings_test_file):
        """Test dry-run mode with specific levels."""
        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "admin-divisions",
                buildings_test_file,
                "output.parquet",
                "--dataset",
                "gaul",
                "--levels",
                "continent,country",
                "--dry-run",
                "--no-cache",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Should only show requested levels (with dataset prefix)
        assert "gaul_continent" in result.output
        assert "gaul_country" in result.output
        # Should not include department
        assert "gaul_department" not in result.output

    def test_add_bbox_dry_run_verbose(self, buildings_test_file):
        """Test dry-run mode with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", buildings_test_file, "output.parquet", "--dry-run", "--verbose"]
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Verbose should not affect dry-run output significantly
        assert "COPY (" in result.output

    def test_dry_run_does_not_create_files(self, buildings_test_file, temp_output_file):
        """Ensure dry-run doesn't create output files."""
        import os

        # Make sure output doesn't exist
        if os.path.exists(temp_output_file):
            os.remove(temp_output_file)

        runner = CliRunner()

        # Test bbox dry-run
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file, "--dry-run"])
        assert result.exit_code == 0
        assert not os.path.exists(temp_output_file)

        # Test admin-divisions dry-run
        result = runner.invoke(
            add,
            ["admin-divisions", buildings_test_file, temp_output_file, "--dry-run", "--no-cache"],
        )
        assert result.exit_code == 0
        assert not os.path.exists(temp_output_file)

    def test_dry_run_with_bbox_column_present(self, places_test_file):
        """Test dry-run when input has bbox column (for admin-divisions)."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["admin-divisions", places_test_file, "output.parquet", "--dry-run", "--no-cache"]
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Should use bbox column for spatial join optimization. Identifiers are
        # quoted by build_spatial_join_condition, so the input bbox struct field
        # renders as `"bbox".xmin` (regression guard for the #460 pre-filter).
        assert '"bbox".xmin' in result.output
        assert "Using bbox columns for optimized spatial join" in result.output
        # Should show spatial join query
        assert "ST_Intersects" in result.output

    def test_dry_run_with_native_geometry_input(self, fields_v2_file):
        """Native-geometry (GeoParquet 2.0) input uses no bbox pre-filter.

        Regression for the #461 review: native inputs null the input bbox column,
        so the ON clause is a bare ST_Intersects (native Parquet stats handle the
        pre-filter) rather than a bbox-overlap predicate.
        """
        runner = CliRunner()
        result = runner.invoke(
            add,
            ["admin-divisions", fields_v2_file, "output.parquet", "--dry-run", "--no-cache"],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        assert "ST_Intersects" in result.output
        # Input bbox should be 'none' for native geometry files
        assert "Bbox columns: none (input)" in result.output
        # No bbox pre-filter in the JOIN ON clause (only ST_Intersects)
        assert "ON ST_Intersects" in result.output
        # #538: native geometry is the fast SPATIAL_JOIN path, not a degraded
        # fallback — the misleading "no bbox optimization" note must be gone.
        assert "Using native geometry with DuckDB SPATIAL_JOIN" in result.output
        assert "no bbox optimization" not in result.output

    def test_dry_run_with_reprojected_bbox_input(self, austria_bbox_covering_file):
        """Reprojected (non-CRS84) input + admin-with-bbox keeps the bbox pre-filter.

        Regression for #538/#540: the input is EPSG:31287 and the admin dataset
        (GAUL) has a bbox column, so admin_divisions reprojects the *admin* side into
        the input CRS (ST_Transform on the admin geometry) and keeps the cheap
        bbox-overlap pre-filter on both sides — the input geometry is left untouched
        (#525). build_spatial_join_condition therefore emits the bbox pre-filter
        ANDed in front of ST_Intersects, so the status line must report the bbox
        optimization, not "no bbox optimization".

        This guards against the f2 mismatch: keying the reported strategy on
        ``source_crs`` alone (ignoring admin_bbox_col) would misreport this join as
        SPATIAL_JOIN_NO_BBOX while the emitted SQL still runs the bbox pre-filter.
        The reported strategy must mirror the ``input_geom_sql is not None`` decision
        in _build_spatial_join_query, i.e. ``source_crs and not _admin_reprojected``.
        """
        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "admin-divisions",
                austria_bbox_covering_file,
                "output.parquet",
                "--dry-run",
                "--no-cache",
            ],
        )

        assert result.exit_code == 0
        assert "DRY RUN MODE" in result.output
        # Admin side is reprojected into the input CRS (ST_Transform on the admin
        # geometry); the input geometry is left as-is so the bbox pre-filter stays.
        assert "ST_Transform" in result.output
        # The bbox-overlap pre-filter must be present in the emitted predicate.
        assert ".xmin <=" in result.output
        # The message must match the predicate that actually runs (bbox pre-filter).
        assert "Using bbox columns for optimized spatial join" in result.output
        assert "no bbox optimization" not in result.output
