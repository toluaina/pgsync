import json

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


class Parent(Base):
    __tablename__ = "parent"
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)


class Surrogate(Base):
    __tablename__ = "surrogate"
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey(Parent.id))


class Child(Base):
    __tablename__ = "child"
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey(Surrogate.id))


class GrandChild(Base):
    __tablename__ = "grand_child"
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey(Child.id))


class GreatGrandChild(Base):
    __tablename__ = "great_grand_child"
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String)
    parent_id = sa.Column(sa.Integer, sa.ForeignKey(GrandChild.id))


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get("database", document["index"])
        create_database(database)
        engine = pg_engine(database=database)
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
