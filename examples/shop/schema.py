import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import config_loader, validate_config


class Base(DeclarativeBase):
    pass


class Shop(Base):
    __tablename__ = "shop"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    city: Mapped[str] = mapped_column(sa.String, nullable=True)


class Department(Base):
    __tablename__ = "department"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    shop_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Shop.id))
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    floor: Mapped[int] = mapped_column(sa.Integer, nullable=True)


class Product(Base):
    __tablename__ = "product"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    department_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Department.id)
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    price: Mapped[float] = mapped_column(sa.Numeric, nullable=True)


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
    validate_config(config)
    teardown(config=config)
    setup(config)


if __name__ == "__main__":
    main()
