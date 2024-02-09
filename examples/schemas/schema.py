import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent"
    __table_args__ = {"schema": "parent"}
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)


class Child(Base):
    __tablename__ = "child"
    __table_args__ = {"schema": "child"}
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Parent.id)
    )


def setup(config: str) -> None:
    for doc in config_loader(config):
        database: str = doc.get("database", doc["index"])
        create_database(database)
        for schema in ("parent", "child"):
            create_schema(database, schema)

        with pg_engine(database) as engine:
            Base.metadata.schema = "parent"
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)

        with pg_engine(database) as engine:
            Base.metadata.schema = "child"
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
