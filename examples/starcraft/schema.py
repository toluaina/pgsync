import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config, load_config

Base = declarative_base()

# sourced from https://starcraft.fandom.com/wiki/List_of_StarCraft_II_units


class Specie(Base):
    __tablename__ = "specie"
    __table_args__ = (UniqueConstraint("name"),)
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)


class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = (
        UniqueConstraint(
            "name",
        ),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    details = sa.Column(sa.String, nullable=True)
    specie_id = sa.Column(sa.Integer, nullable=False)


class Structure(Base):
    __tablename__ = "structure"
    __table_args__ = (
        UniqueConstraint(
            "name",
        ),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    details = sa.Column(sa.String, nullable=True)
    specie_id = sa.Column(sa.Integer, nullable=False)


def setup(config: str) -> None:
    for document in load_config(config):
        database: str = document.get("database", document["index"])
        create_database(database)
        with pg_engine(database) as engine:
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
