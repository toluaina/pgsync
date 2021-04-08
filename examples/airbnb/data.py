import json
from datetime import datetime, timedelta

import click
from schema import Bookings, Cities, Countries, Hosts, Places, Reviews, Users
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import get_config


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
    engine = pg_engine(database=document[0].get('database', document[0]['index']))
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    users = [
        Users(email='stephanie.miller@aol.com'),
        Users(email='nancy.gaines@ibm.com'),
        Users(email='andrea.cabrera@gmail.com'),
        Users(email='brandon86@yahoo.com'),
        Users(email='traci.williams@amazon.com'),
        Users(email='john.brown@apple.com'),
    ]

    hosts = [
        Hosts(email='kermit@muppet-labs.inc'),
        Hosts(email='bert@sesame.street'),
        Hosts(email='big.bird@sesame.street'),
        Hosts(email='cookie.monster@sesame.street'),
        Hosts(email='mr.snuffleupagus@sesame.street'),
        Hosts(email='grover@sesame.street'),
        Hosts(email='miss.piggy@muppet-labs.inc'),
    ]

    cities = [
        Cities(
            name='Manila',
            country=Countries(
                name='Philippines',
                country_code='PH',
            )
        ),
        Cities(
            name='Lisbon',
            country=Countries(
                name='Portugal',
                country_code='PT',
            ),
        ),
        Cities(
            name='Havana',
            country=Countries(
                name='Cuba',
                country_code='CU',
            ),
        ),
        Cities(
            name='Copenhagen',
            country=Countries(
                name='Denmark',
                country_code='DK',
            ),
        ),
        Cities(
            name='London',
            country=Countries(
                name='United Kingdom',
                country_code='UK',
            ),
        ),
        Cities(
            name='Casablanca',
            country=Countries(
                name='Morocco',
                country_code='MA',
            ),
        ),
    ]

    places = [
        Places(
            host=hosts[0],
            city=cities[0],
            address='Quezon Boulevard',
        ),
        Places(
            host=hosts[1],
            city=cities[1],
            address='Castelo de São Jorge',
        ),
        Places(
            host=hosts[2],
            city=cities[2],
            address='Old Havana',
        ),
        Places(
            host=hosts[3],
            city=cities[3],
            address='Tivoli Gardens',
        ),
        Places(
            host=hosts[4],
            city=cities[4],
            address='Buckingham Palace',
        ),
        Places(
            host=hosts[5],
            city=cities[5],
            address='Medina',
        ),
    ]

    reviews = [
        Reviews(
            booking=Bookings(
                user=users[0],
                place=places[0],
                start_date=datetime.now() + timedelta(days=1),
                end_date=datetime.now() + timedelta(days=4),
                price_per_night=100,
                num_nights=4,
            ),
            rating=1,
            review_body='The rooms were left in a tolerable state',
        ),
        Reviews(
            booking=Bookings(
                user=users[1],
                place=places[1],
                start_date=datetime.now() + timedelta(days=2),
                end_date=datetime.now() + timedelta(days=4),
                price_per_night=150,
                num_nights=3,
            ),
            rating=2,
            review_body='I found my place wonderfully taken care of',
        ),
        Reviews(
            booking=Bookings(
                user=users[2],
                place=places[2],
                start_date=datetime.now() + timedelta(days=15),
                end_date=datetime.now() + timedelta(days=19),
                price_per_night=120,
                num_nights=4,
            ),
            rating=3,
            review_body='All of my house rules were respected',
        ),
        Reviews(
            booking=Bookings(
                user=users[3],
                place=places[3],
                start_date=datetime.now() + timedelta(days=2),
                end_date=datetime.now() + timedelta(days=7),
                price_per_night=300,
                num_nights=5,
            ),
            rating=4,
            review_body='Such a pleasure to host and welcome these guests',
        ),
        Reviews(
            booking=Bookings(
                user=users[4],
                place=places[4],
                start_date=datetime.now() + timedelta(days=1),
                end_date=datetime.now() + timedelta(days=10),
                price_per_night=800,
                num_nights=3,
            ),
            rating=5,
            review_body='We would be happy to host them again',
        ),
        Reviews(
            booking=Bookings(
                user=users[5],
                place=places[5],
                start_date=datetime.now() + timedelta(days=2),
                end_date=datetime.now() + timedelta(days=8),
                price_per_night=80,
                num_nights=10,
            ),
            rating=3,
            review_body='Please do not visit our town ever again!',
        ),
    ]

    with subtransactions(session):
        session.add_all(users)
        session.add_all(hosts)
        session.add_all(cities)
        session.add_all(places)
        session.add_all(reviews)


if __name__ == '__main__':
    main()
