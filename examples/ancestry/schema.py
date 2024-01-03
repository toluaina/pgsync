import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)


class Surrogate(Base):
    __tablename__ = "surrogate"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Parent.id)
    )


class Child(Base):
    __tablename__ = "child"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Surrogate.id)
    )


class GrandChild(Base):
    __tablename__ = "grand_child"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Child.id))


class GreatGrandChild(Base):
    __tablename__ = "great_grand_child"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(GrandChild.id)
    )


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
def main(config):
    config: str = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
