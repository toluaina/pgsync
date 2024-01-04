import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customer"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class Group(Base):
    __tablename__ = "group"
    __table_args__ = (
        UniqueConstraint(
            "group_name",
        ),
    )
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    group_name: Mapped[str] = mapped_column(sa.String, nullable=False)


class CustomerGroup(Base):
    __tablename__ = "customer_group"
    __table_args__ = (
        UniqueConstraint(
            "customer_id",
            "group_id",
        ),
    )
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    customer_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Customer.id, ondelete="CASCADE"),
    )
    customer: Mapped[Customer] = sa.orm.relationship(
        Customer,
        backref=sa.orm.backref("customers"),
    )
    group_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Group.id, ondelete="CASCADE"),
    )
    group: Mapped[Group] = sa.orm.relationship(
        Group,
        backref=sa.orm.backref("groups"),
    )


def setup(config: str) -> None:
    for doc in config_loader(config):
        database: str = doc.get("database", doc["index"])
        schema: str = doc.get("schema", DEFAULT_SCHEMA)
        create_database(database)
        create_schema(database, schema)
        with pg_engine(database) as engine:
            Base.metadata.schema = schema
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
