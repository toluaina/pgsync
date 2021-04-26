import json

import click
import sqlalchemy as sa
from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Middle(Base):
    __tablename__ = 'middle'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)


class Child(Base):
    __tablename__ = 'child'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    middle_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(Middle.id),
    )


class Parent(Base):
    __tablename__ = 'parent'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    middle_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(Middle.id),
    )


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get('database', document['index'])
        create_database(database)
        engine = pg_engine(database=database)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == '__main__':
    main()
