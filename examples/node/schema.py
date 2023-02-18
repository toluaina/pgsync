import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config

Base = declarative_base()


class Node(Base):
    __tablename__ = "node"
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)
    node_id = sa.Column(sa.Integer, sa.ForeignKey("node.id"))
    children = sa.orm.relationship("Node", lazy="joined", join_depth=2)


def setup(config: str) -> None:
    for document in config_loader(config):
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
    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
