import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


# sourced from https://starcraft.fandom.com/wiki/List_of_StarCraft_II_units


class Specie(Base):
    __tablename__ = "specie"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = (
        UniqueConstraint(
            "name",
        ),
    )
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    details: Mapped[str] = mapped_column(sa.String, nullable=True)
    specie_id: Mapped[int] = mapped_column(sa.Integer, nullable=False)


class Structure(Base):
    __tablename__ = "structure"
    __table_args__ = (
        UniqueConstraint(
            "name",
        ),
    )
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    details: Mapped[str] = mapped_column(sa.String, nullable=True)
    specie_id: Mapped[int] = mapped_column(sa.Integer, nullable=False)


def setup(config: str) -> None:
    for doc in config_loader(config):
        database: str = doc.get("database", doc["index"])
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
def main(config: str) -> None:
    config: str = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
