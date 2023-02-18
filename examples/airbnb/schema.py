from datetime import datetime

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config

Base = declarative_base()


class User(Base):
    __tablename__ = "user"
    __table_args__ = (UniqueConstraint("email"),)
    id = sa.Column(sa.Integer, primary_key=True)
    email = sa.Column(sa.String, unique=True, nullable=False)


class Host(Base):
    __tablename__ = "host"
    __table_args__ = (UniqueConstraint("email"),)
    id = sa.Column(sa.Integer, primary_key=True)
    email = sa.Column(sa.String, unique=True, nullable=False)


class Country(Base):
    __tablename__ = "country"
    __table_args__ = (UniqueConstraint("name", "country_code"),)
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    country_code = sa.Column(sa.String, nullable=False)


class City(Base):
    __tablename__ = "city"
    __table_args__ = (UniqueConstraint("name", "country_id"),)
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    country_id = sa.Column(sa.Integer, sa.ForeignKey(Country.id))
    country = sa.orm.relationship(
        Country,
        backref=sa.orm.backref("country"),
    )


class Place(Base):
    __tablename__ = "place"
    __table_args__ = (UniqueConstraint("host_id", "address", "city_id"),)
    id = sa.Column(sa.Integer, primary_key=True)
    host_id = sa.Column(sa.Integer, sa.ForeignKey(Host.id))
    address = sa.Column(sa.String, nullable=False)
    city_id = sa.Column(sa.Integer, sa.ForeignKey(City.id))
    host = sa.orm.relationship(
        Host,
        backref=sa.orm.backref("host"),
    )
    city = sa.orm.relationship(
        City,
        backref=sa.orm.backref("city"),
    )


class Booking(Base):
    __tablename__ = "booking"
    __table_args__ = (UniqueConstraint("user_id", "place_id", "start_date"),)
    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Integer, sa.ForeignKey(User.id))
    place_id = sa.Column(sa.Integer, sa.ForeignKey(Place.id))
    start_date = sa.Column(sa.DateTime, default=datetime.now())
    end_date = sa.Column(sa.DateTime, default=datetime.now())
    price_per_night = sa.Column(sa.Float, default=0)
    num_nights = sa.Column(sa.Integer, nullable=False, default=1)
    user = sa.orm.relationship(User)
    place = sa.orm.relationship(Place)


class Review(Base):
    __tablename__ = "review"
    __table_args__ = (UniqueConstraint("booking_id"),)
    id = sa.Column(sa.Integer, primary_key=True)
    booking_id = sa.Column(sa.Integer, sa.ForeignKey(Booking.id))
    rating = sa.Column(sa.SmallInteger, nullable=True)
    review_body = sa.Column(sa.Text, nullable=True)
    booking = sa.orm.relationship(
        Booking,
        backref=sa.orm.backref("booking"),
    )


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
    config: str = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
