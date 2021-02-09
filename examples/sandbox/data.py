import json

import click
from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import get_config
from schema import Zone, ZoneRelationParent
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(drop_db=False, config=config)
    document = json.load(open(config))
    engine = pg_engine(
        database=document[0].get('database', document[0]['index'])
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    zones = [
        Zone(id='Zone A', cassini_id="cassini 1", country_code='AA', admin_level='super_user'),
        Zone(id='Zone B', cassini_id="cassini 2", country_code='BB', admin_level='super_user'),
        Zone(id='Zone C', cassini_id="cassini 3", country_code='CC', admin_level='nobody'),
        Zone(id='Zone D', cassini_id="cassini 4", country_code='CC', admin_level='super_user'),
        Zone(id='Zone E', cassini_id="cassini 5", country_code='EE', admin_level='nobody'),
        Zone(id='Zone A1', cassini_id="cassini 6", country_code='FF', admin_level='nobody'),
        Zone(id='Zone B1', cassini_id="cassini 7", country_code='GG', admin_level='super_user'),
        Zone(id='Zone C1', cassini_id="cassini 8", country_code='HH', admin_level='nobody'),
        Zone(id='Zone D1', cassini_id="cassini 9", country_code='II', admin_level='nobody'),
        Zone(id='Zone E1', cassini_id="cassini 10", country_code='JJ', admin_level='super_user'),
    ]
    with subtransactions(session):
        session.add_all(zones)

    zone_relation_parents = [
        ZoneRelationParent(
            zone_id=zones[0].id,
            zone_id_relation=zones[5].id,
        ),
        ZoneRelationParent(
            zone_id=zones[1].id,
            zone_id_relation=zones[6].id,
        ),
        ZoneRelationParent(
            zone_id=zones[2].id,
            zone_id_relation=zones[7].id,
        ),
        ZoneRelationParent(
            zone_id=zones[3].id,
            zone_id_relation=zones[8].id,
        ),
        ZoneRelationParent(
            zone_id=zones[4].id,
            zone_id_relation=zones[9].id,
        ),
    ]
    with subtransactions(session):
        session.add_all(zone_relation_parents)


if __name__ == '__main__':
    main()
