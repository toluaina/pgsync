from datetime import datetime

import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "user"
    __table_args__ = (UniqueConstraint("email"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    email: Mapped[str] = mapped_column(sa.String, unique=True, nullable=False)


class Host(Base):
    __tablename__ = "host"
    __table_args__ = (UniqueConstraint("email"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    email: Mapped[str] = mapped_column(sa.String, unique=True, nullable=False)


class Country(Base):
    __tablename__ = "country"
    __table_args__ = (UniqueConstraint("name", "country_code"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    country_code: Mapped[str] = mapped_column(sa.String, nullable=False)


class City(Base):
    __tablename__ = "city"
    __table_args__ = (UniqueConstraint("name", "country_id"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    country_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Country.id)
    )
    country: Mapped[Country] = sa.orm.relationship(
        Country,
        backref=sa.orm.backref("country"),
    )


class Place(Base):
    __tablename__ = "place"
    __table_args__ = (UniqueConstraint("host_id", "address", "city_id"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    host_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Host.id))
    address: Mapped[str] = mapped_column(sa.String, nullable=False)
    city_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(City.id))
    host: Mapped[Host] = sa.orm.relationship(
        Host,
        backref=sa.orm.backref("host"),
    )
    city: Mapped[City] = sa.orm.relationship(
        City,
        backref=sa.orm.backref("city"),
    )


class Booking(Base):
    __tablename__ = "booking"
    __table_args__ = (UniqueConstraint("user_id", "place_id", "start_date"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(User.id))
    place_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Place.id))
    start_date: Mapped[datetime] = mapped_column(
        sa.DateTime, default=datetime.now()
    )
    end_date: Mapped[datetime] = mapped_column(
        sa.DateTime, default=datetime.now()
    )
    price_per_night: Mapped[float] = mapped_column(sa.Float, default=0)
    num_nights: Mapped[int] = mapped_column(
        sa.Integer, nullable=False, default=1
    )
    user: Mapped[User] = sa.orm.relationship(User)
    place: Mapped[Place] = sa.orm.relationship(Place)


class Review(Base):
    __tablename__ = "review"
    __table_args__ = (UniqueConstraint("booking_id"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    booking_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Booking.id)
    )
    rating: Mapped[int] = mapped_column(sa.SmallInteger, nullable=True)
    review_body: Mapped[str] = mapped_column(sa.Text, nullable=True)
    booking: Mapped[Booking] = sa.orm.relationship(
        Booking,
        backref=sa.orm.backref("booking"),
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
    config: str = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
