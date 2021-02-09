import json

import click
import sqlalchemy as sa
from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Zone(Base):
    __tablename__ = 'zone'
    __table_args__ = ()
    id = sa.Column(sa.String, primary_key=True)
    country_code = sa.Column(sa.String)
    admin_level = sa.Column(sa.String)
    geometry_json = sa.Column(sa.JSON)
    bounding_box_json = sa.Column(sa.JSON)
    cassini_id = sa.Column(sa.String)


class ZoneRelationParent(Base):
    __tablename__ = 'zone_relation_parent'
    __table_args__ = ()
    id = sa.Column(sa.Integer, primary_key=True)
    zone_id = sa.Column(sa.String, sa.ForeignKey(Zone.id))
    zone_id_relation = sa.Column(sa.String, sa.ForeignKey(Zone.id))


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
