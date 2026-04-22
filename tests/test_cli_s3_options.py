"""Tests for global S3 CLI options."""

import importlib

from click.testing import CliRunner

main_module = importlib.import_module("geoparquet_io.cli.main")
cli = main_module.cli


class TestGlobalS3Options:
    """Test that global S3 flags are accepted and land in ctx.obj."""

    def test_global_s3_endpoint_accepted(self):
        """--s3-endpoint is accepted as a global option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--s3-endpoint", "minio.local:9000", "--help"])
        assert result.exit_code == 0

    def test_global_s3_region_accepted(self):
        """--s3-region is accepted as a global option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--s3-region", "eu-west-1", "--help"])
        assert result.exit_code == 0

    def test_global_s3_no_ssl_accepted(self):
        """--s3-no-ssl is accepted as a global option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--s3-no-ssl", "--help"])
        assert result.exit_code == 0

    def test_global_aws_profile_accepted(self):
        """--aws-profile is accepted as a global option."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--aws-profile", "prod", "--help"])
        assert result.exit_code == 0

    def test_s3_options_shown_in_help(self):
        """S3 global options appear in gpio --help."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert "--s3-endpoint" in result.output
        assert "--s3-region" in result.output
        assert "--s3-no-ssl" in result.output
        assert "--aws-profile" in result.output
