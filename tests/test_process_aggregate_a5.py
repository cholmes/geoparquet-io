from click.testing import CliRunner

from geoparquet_io.cli.main import cli


def test_process_aggregate_group_exists():
    runner = CliRunner()
    result = runner.invoke(cli, ["process", "aggregate", "--help"])
    assert result.exit_code == 0
    assert "aggregate" in result.output.lower()
