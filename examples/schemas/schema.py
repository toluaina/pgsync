import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config

Base = declarative_base()


class Parent(Base):
    __tablename__ = "parent"
    __table_args__ = {"schema": "parent"}
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)


class Child(Base):
    __tablename__ = "child"
    __table_args__ = {"schema": "child"}
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey(Parent.id))


def setup(config: str) -> None:
    for document in config_loader(config):
        database: str = document.get("database", document["index"])
        create_database(database)
        for schema in ("parent", "child"):
            create_schema(database, schema)

        with pg_engine(database) as engine:
            engine: sa.engine.Engine = engine.connect().execution_options(
                schema_translate_map={None: "parent"}
            )
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)

        with pg_engine(database) as engine:
            engine: sa.engine.Engine = engine.connect().execution_options(
                schema_translate_map={None: "child"}
            )
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config):

    config: str = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
