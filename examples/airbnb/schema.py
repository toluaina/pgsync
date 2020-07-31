import json
from datetime import datetime

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


class Users(Base):
    __tablename__ = 'users'
    __table_args__ = (
        UniqueConstraint('email'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    email = sa.Column(sa.String, unique=True, nullable=False)


class Hosts(Base):
    __tablename__ = 'hosts'
    __table_args__ = (
        UniqueConstraint('email'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    email = sa.Column(sa.String, unique=True, nullable=False)


class Countries(Base):
    __tablename__ = 'countries'
    __table_args__ = (
        UniqueConstraint('name', 'country_code'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    country_code = sa.Column(sa.String, nullable=False)


class Cities(Base):
    __tablename__ = 'cities'
    __table_args__ = (
        UniqueConstraint('name', 'country_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False)
    country_id = sa.Column(
        sa.Integer, sa.ForeignKey(Countries.id)
    )
    country = sa.orm.relationship(
        Countries,
        backref=sa.orm.backref('countries')
    )


class Places(Base):
    __tablename__ = 'places'
    __table_args__ = (
        UniqueConstraint('host_id', 'address', 'city_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    host_id = sa.Column(sa.Integer, sa.ForeignKey(Hosts.id))
    address = sa.Column(sa.String, nullable=False)
    city_id = sa.Column(sa.Integer, sa.ForeignKey(Cities.id))
    host = sa.orm.relationship(
        Hosts,
        backref=sa.orm.backref('hosts')
    )
    city = sa.orm.relationship(
        Cities,
        backref=sa.orm.backref('cities')
    )


class Bookings(Base):
    __tablename__ = 'bookings'
    __table_args__ = (
        UniqueConstraint('user_id', 'place_id', 'start_date'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Integer, sa.ForeignKey(Users.id))
    place_id = sa.Column(sa.Integer, sa.ForeignKey(Places.id))
    start_date = sa.Column(sa.DateTime, default=datetime.now())
    end_date = sa.Column(sa.DateTime, default=datetime.now())
    price_per_night = sa.Column(sa.Float, default=0)
    num_nights = sa.Column(sa.Integer, nullable=False, default=1)
    user = sa.orm.relationship(Users)
    place = sa.orm.relationship(Places)


class Reviews(Base):
    __tablename__ = 'reviews'
    __table_args__ = (
        UniqueConstraint('booking_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    booking_id = sa.Column(sa.Integer, sa.ForeignKey(Bookings.id))
    rating = sa.Column(sa.SmallInteger, nullable=True)
    review_body = sa.Column(sa.Text, nullable=True)
    booking = sa.orm.relationship(
        Bookings,
        backref=sa.orm.backref('bookings')
    )


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get('database', document['index'])
        create_database(database)
        # create schema
        engine = pg_engine(database=database)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)


@click.command()
@click.option('--config', '-c', help='Schema config')
def main(config):

    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == '__main__':
    main()
