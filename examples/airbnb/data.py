import typing as t
from datetime import datetime, timedelta

import click
from schema import Booking, City, Country, Host, Place, Review, User
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config):
    config: str = get_config(config)
    teardown(drop_db=False, config=config)
    doc: dict = next(config_loader(config))
    database: str = doc.get("database", doc["index"])
    with pg_engine(database) as engine:
        Session = sessionmaker(bind=engine, autoflush=True)
        session = Session()

        users: t.List[User] = [
            User(email="stephanie.miller@aol.com"),
            User(email="nancy.gaines@ibm.com"),
            User(email="andrea.cabrera@gmail.com"),
            User(email="brandon86@yahoo.com"),
            User(email="traci.williams@amazon.com"),
            User(email="john.brown@apple.com"),
        ]

        hosts: t.List[Host] = [
            Host(email="kermit@muppet-labs.inc"),
            Host(email="bert@sesame.street"),
            Host(email="big.bird@sesame.street"),
            Host(email="cookie.monster@sesame.street"),
            Host(email="mr.snuffleupagus@sesame.street"),
            Host(email="grover@sesame.street"),
            Host(email="miss.piggy@muppet-labs.inc"),
        ]

        cities: t.List[City] = [
            City(
                name="Manila",
                country=Country(
                    name="Philippines",
                    country_code="PH",
                ),
            ),
            City(
                name="Lisbon",
                country=Country(
                    name="Portugal",
                    country_code="PT",
                ),
            ),
            City(
                name="Havana",
                country=Country(
                    name="Cuba",
                    country_code="CU",
                ),
            ),
            City(
                name="Copenhagen",
                country=Country(
                    name="Denmark",
                    country_code="DK",
                ),
            ),
            City(
                name="London",
                country=Country(
                    name="United Kingdom",
                    country_code="UK",
                ),
            ),
            City(
                name="Casablanca",
                country=Country(
                    name="Morocco",
                    country_code="MA",
                ),
            ),
        ]

        places: t.List[Place] = [
            Place(
                host=hosts[0],
                city=cities[0],
                address="Quezon Boulevard",
            ),
            Place(
                host=hosts[1],
                city=cities[1],
                address="Castelo de São Jorge",
            ),
            Place(
                host=hosts[2],
                city=cities[2],
                address="Old Havana",
            ),
            Place(
                host=hosts[3],
                city=cities[3],
                address="Tivoli Gardens",
            ),
            Place(
                host=hosts[4],
                city=cities[4],
                address="Buckingham Palace",
            ),
            Place(
                host=hosts[5],
                city=cities[5],
                address="Medina",
            ),
        ]

        reviews: t.List[Review] = [
            Review(
                booking=Booking(
                    user=users[0],
                    place=places[0],
                    start_date=datetime.now() + timedelta(days=1),
                    end_date=datetime.now() + timedelta(days=4),
                    price_per_night=100,
                    num_nights=4,
                ),
                rating=1,
                review_body="The rooms were left in a tolerable state",
            ),
            Review(
                booking=Booking(
                    user=users[1],
                    place=places[1],
                    start_date=datetime.now() + timedelta(days=2),
                    end_date=datetime.now() + timedelta(days=4),
                    price_per_night=150,
                    num_nights=3,
                ),
                rating=2,
                review_body="I found my place wonderfully taken care of",
            ),
            Review(
                booking=Booking(
                    user=users[2],
                    place=places[2],
                    start_date=datetime.now() + timedelta(days=15),
                    end_date=datetime.now() + timedelta(days=19),
                    price_per_night=120,
                    num_nights=4,
                ),
                rating=3,
                review_body="All of my house rules were respected",
            ),
            Review(
                booking=Booking(
                    user=users[3],
                    place=places[3],
                    start_date=datetime.now() + timedelta(days=2),
                    end_date=datetime.now() + timedelta(days=7),
                    price_per_night=300,
                    num_nights=5,
                ),
                rating=4,
                review_body="Such a pleasure to host and welcome these guests",
            ),
            Review(
                booking=Booking(
                    user=users[4],
                    place=places[4],
                    start_date=datetime.now() + timedelta(days=1),
                    end_date=datetime.now() + timedelta(days=10),
                    price_per_night=800,
                    num_nights=3,
                ),
                rating=5,
                review_body="We would be happy to host them again",
            ),
            Review(
                booking=Booking(
                    user=users[5],
                    place=places[5],
                    start_date=datetime.now() + timedelta(days=2),
                    end_date=datetime.now() + timedelta(days=8),
                    price_per_night=80,
                    num_nights=10,
                ),
                rating=3,
                review_body="Please do not visit our town ever again!",
            ),
        ]

        with subtransactions(session):
            session.add_all(users)
            session.add_all(hosts)
            session.add_all(cities)
            session.add_all(places)
            session.add_all(reviews)


if __name__ == "__main__":
    main()
