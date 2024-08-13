import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "node"
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    node_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey("node.id"))
    children: Mapped["Node"] = sa.orm.relationship(
        "Node", lazy="joined", join_depth=2
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
def main(config: str) -> None:
    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
