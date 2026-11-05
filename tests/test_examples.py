"""Tests for the examples runner (python -m examples.run)."""

import json
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from examples.run import cli, create_tables, drop, Example, seed_rows
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.sync import Sync

WITH_DATA = [name for name in Example.names() if Example(name).has_data]


@pytest.fixture
def config(tmp_path, request):
    """A copy of the example config, with a test database and index."""
    name: str = request.param
    docs: list = json.loads((Example(name).path / "schema.json").read_text())
    for doc in docs:
        doc["database"] = doc["index"] = f"test_example_{name}"
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(docs))
    source: dict = {
        "config": str(path),
        "schema_url": None,
        "s3_schema_url": None,
    }
    yield name, docs, source
    drop(source)


@pytest.mark.skipif(
    IS_MYSQL_COMPAT,
    reason="Skipped because IS_MYSQL_COMPAT env var is set",
)
class TestExamples:
    @pytest.mark.parametrize("config", WITH_DATA, indirect=True)
    def test_example_syncs(self, config):
        name, docs, source = config
        example = Example(name)
        drop(source)
        for doc in docs:
            create_tables(example, doc)
            seed_rows(example, doc, size=0)
            documents = list(Sync(doc, validate=False).sync())
            assert documents, f"{name}: no documents"
            assert all(document["_id"] for document in documents)

    @pytest.mark.parametrize("config", ["book"], indirect=True)
    def test_size_adds_random_books(self, config):
        name, docs, source = config
        example = Example(name)
        drop(source)
        create_tables(example, docs[0])
        seed_rows(example, docs[0], size=50)
        assert len(list(Sync(docs[0], validate=False).sync())) == 58


class TestCli:
    def test_list(self):
        result = CliRunner().invoke(cli, ["list"])
        assert result.exit_code == 0
        for name in Example.names():
            assert name in result.output
        assert "schema only" in result.output

    def test_every_example_with_data_has_a_seed(self):
        for name in WITH_DATA:
            example = Example(name)
            assert callable(example.data.seed), name
            assert example.models.__doc__, name

    def test_size_needs_support(self):
        result = CliRunner().invoke(cli, ["seed", "node", "--size", "5"])
        assert result.exit_code == 2
        assert "does not support --size" in result.output

    def test_setup_needs_data(self):
        result = CliRunner().invoke(cli, ["setup", "airline"])
        assert result.exit_code == 2
        assert "has no sample data" in result.output

    def test_one_config_source(self):
        result = CliRunner().invoke(
            cli,
            [
                "down",
                "book",
                "--schema_url",
                "http://x",
                "--s3_schema_url",
                "s3://y",
            ],
        )
        assert result.exit_code == 2
        assert "Use only one of" in result.output

    @pytest.mark.skipif(
        IS_MYSQL_COMPAT,
        reason="Skipped because IS_MYSQL_COMPAT env var is set",
    )
    @patch("examples.run.os.execv")
    @patch("examples.run.subprocess.run")
    def test_up_without_data_runs_bootstrap_and_pgsync(self, run, execv):
        result = CliRunner().invoke(cli, ["up", "airline", "--", "--monitor"])
        assert result.exit_code == 0, result.output
        config: str = str(Example("airline").path / "schema.json")
        bootstrap_args = run.call_args.args[0]
        assert bootstrap_args[1].endswith("bin/bootstrap")
        assert bootstrap_args[2:] == ["--config", config]
        pgsync_args = execv.call_args.args[1]
        assert pgsync_args[1].endswith("bin/pgsync")
        assert pgsync_args[2:] == ["--config", config, "--daemon", "--monitor"]
