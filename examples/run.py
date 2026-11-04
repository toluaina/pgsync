"""Set up and run the PGSync examples.

    python -m examples.run list
    python -m examples.run up book

Each example is a folder with a schema.json. An example with sample data
also has models.py (the tables) and data.py (a seed function).
"""

import functools
import importlib
import inspect
import os
import subprocess
import sys
import typing as t
from pathlib import Path
from types import ModuleType

import click
from sqlalchemy.orm import Session

from pgsync.base import (
    create_database,
    create_schema,
    database_exists,
    pg_engine,
)
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
from pgsync.settings import IS_MYSQL_COMPAT
from pgsync.sync import Sync
from pgsync.utils import config_loader, validate_config

ROOT: Path = Path(__file__).resolve().parent
BIN: Path = ROOT.parent / "bin"


class Example(object):
    """A folder in examples/ with a schema.json."""

    def __init__(self, name: str):
        self.name: str = name
        self.path: Path = ROOT / name

    @staticmethod
    def names() -> t.List[str]:
        return sorted(path.parent.name for path in ROOT.glob("*/schema.json"))

    @property
    def has_data(self) -> bool:
        return (self.path / "models.py").exists()

    @property
    def models(self) -> ModuleType:
        return importlib.import_module(f"examples.{self.name}.models")

    @property
    def data(self) -> ModuleType:
        return importlib.import_module(f"examples.{self.name}.data")

    @property
    def summary(self) -> str:
        if not self.has_data:
            return "schema only: load the data as in examples/README.md"
        return (self.models.__doc__ or "").strip()


def _database(doc: dict) -> str:
    return doc.get("database", doc["index"])


def _schema_map(schema: str) -> dict:
    # put the tables without an explicit schema in the config schema
    return {} if IS_MYSQL_COMPAT else {None: schema}


def drop(source: dict, drop_db: bool = True) -> None:
    """Drop the database (or only its rows), the index, and the state."""
    validate_config(**source)
    if drop_db and not IS_MYSQL_COMPAT:
        for doc in config_loader(**source):
            if database_exists(_database(doc)):
                # a replication slot blocks DROP DATABASE
                sync: Sync = Sync(doc, validate=False)
                sync.drop_replication_slot(sync.slot_name)
    teardown(drop_db=drop_db, **source)


def create_tables(example: Example, doc: dict) -> None:
    database: str = _database(doc)
    schema: str = doc.get("schema", DEFAULT_SCHEMA)
    models: ModuleType = example.models
    create_database(database)
    for name in {schema} | {
        table.schema
        for table in models.Base.metadata.tables.values()
        if table.schema
    }:
        create_schema(database, name)
    with pg_engine(database) as engine, engine.begin() as connection:
        connection = connection.execution_options(
            schema_translate_map=_schema_map(schema)
        )
        models.Base.metadata.create_all(connection)
        if hasattr(models, "after_create"):
            models.after_create(connection, schema)


def seed_rows(example: Example, doc: dict, size: int) -> None:
    schema: str = doc.get("schema", DEFAULT_SCHEMA)
    kwargs: dict = {"size": size} if size else {}
    with pg_engine(_database(doc)) as engine, engine.connect() as connection:
        connection = connection.execution_options(
            schema_translate_map=_schema_map(schema)
        )
        with Session(bind=connection) as session:
            example.data.seed(session, **kwargs)
            session.commit()
    # a materialized view shows the new rows only after a refresh
    Sync(doc, validate=False).refresh_views()


def with_example(func: t.Callable) -> t.Callable:
    """Add the example name and the schema config options to a command."""

    @click.argument(
        "name", metavar="EXAMPLE", type=click.Choice(Example.names())
    )
    @click.option(
        "--config",
        "-c",
        type=click.Path(exists=True, dir_okay=False),
        help="Schema config  [default: the example's schema.json]",
    )
    @click.option("--schema_url", help="URL for schema config")
    @click.option("--s3_schema_url", help="S3 URL for schema config")
    @functools.wraps(func)
    def command(
        name: str,
        config: t.Optional[str],
        schema_url: t.Optional[str],
        s3_schema_url: t.Optional[str],
        **kwargs,
    ) -> None:
        if sum(map(bool, (config, schema_url, s3_schema_url))) > 1:
            raise click.UsageError(
                "Use only one of --config, --schema_url, and --s3_schema_url."
            )
        example: Example = Example(name)
        if not (schema_url or s3_schema_url):
            config = config or str(example.path / "schema.json")
        source: dict = {
            "config": config,
            "schema_url": schema_url,
            "s3_schema_url": s3_schema_url,
        }
        func(example, source, **kwargs)

    return command


def _require_data(example: Example) -> None:
    if not example.has_data:
        raise click.UsageError(
            f"The {example.name} example has no sample data. "
            "Load it as described in examples/README.md."
        )


def _size_option(func: t.Callable) -> t.Callable:
    return click.option(
        "--size",
        "-n",
        type=click.IntRange(min=0),
        default=0,
        help="Random rows to add (book only)",
    )(func)


def _check_size(example: Example, size: int) -> None:
    if size and "size" not in inspect.signature(example.data.seed).parameters:
        raise click.UsageError(
            f"The {example.name} example does not support --size."
        )


def _cli_args(source: dict) -> t.List[str]:
    args: t.List[str] = []
    for key, value in source.items():
        if value:
            args += [f"--{key}", value]
    return args


@click.group()
def cli() -> None:
    """Set up and run the PGSync examples."""


@cli.command("list")
def list_examples() -> None:
    """Show the examples."""
    names: t.List[str] = Example.names()
    width: int = max(map(len, names))
    for name in names:
        click.echo(f"{name:<{width}}  {Example(name).summary}")


@cli.command()
@with_example
def setup(example: Example, source: dict) -> None:
    """Create the database and the tables (drop them first)."""
    _require_data(example)
    drop(source)
    for doc in config_loader(**source):
        create_tables(example, doc)
    click.echo(f"Created the {example.name} tables.")


@cli.command()
@with_example
@_size_option
def seed(example: Example, source: dict, size: int) -> None:
    """Replace the rows with the sample data."""
    _require_data(example)
    _check_size(example, size)
    drop(source, drop_db=False)
    for doc in config_loader(**source):
        seed_rows(example, doc, size)
    click.echo(f"Added the {example.name} sample data.")


@cli.command()
@with_example
def down(example: Example, source: dict) -> None:
    """Drop the database, the index, and the sync state."""
    drop(source)
    click.echo(f"Removed the {example.name} example.")


@cli.command()
@with_example
@_size_option
@click.argument("pgsync_args", nargs=-1, type=click.UNPROCESSED)
def up(
    example: Example,
    source: dict,
    size: int,
    pgsync_args: t.Tuple[str, ...],
) -> None:
    """Set up the example, then run pgsync as a daemon.

    Put extra pgsync options after --, for example: up book -- --monitor
    """
    if example.has_data:
        _check_size(example, size)
        drop(source)
        for doc in config_loader(**source):
            create_tables(example, doc)
            seed_rows(example, doc, size)
    args: t.List[str] = _cli_args(source)
    if not IS_MYSQL_COMPAT:
        subprocess.run(
            [sys.executable, str(BIN / "bootstrap"), *args], check=True
        )
    sys.stdout.flush()
    os.execv(
        sys.executable,
        [sys.executable, str(BIN / "pgsync"), *args, "--daemon", *pgsync_args],
    )


if __name__ == "__main__":
    cli()
