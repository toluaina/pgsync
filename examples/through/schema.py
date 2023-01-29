import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config

Base = declarative_base()


class Customer(Base):
    __tablename__ = "customer"
    __table_args__ = (UniqueConstraint("name"),)
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)


class Group(Base):
    __tablename__ = "group"
    __table_args__ = (
        UniqueConstraint(
            "group_name",
        ),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    group_name = sa.Column(sa.String, nullable=False)


class CustomerGroup(Base):
    __tablename__ = "customer_group"
    __table_args__ = (
        UniqueConstraint(
            "customer_id",
            "group_id",
        ),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    customer_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(Customer.id, ondelete="CASCADE"),
    )
    customer = sa.orm.relationship(
        Customer,
        backref=sa.orm.backref("customers"),
    )
    group_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(Group.id, ondelete="CASCADE"),
    )
    group = sa.orm.relationship(
        Group,
        backref=sa.orm.backref("groups"),
    )


def setup(config: str) -> None:
    for document in config_loader(config):
        database: str = document.get("database", document["index"])
        schema: str = document.get("schema", DEFAULT_SCHEMA)
        create_database(database)
        create_schema(database, schema)
        with pg_engine(database) as engine:
            engine = engine.connect().execution_options(
                schema_translate_map={None: schema}
            )
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
