import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config
from pgsync.view import CreateView

Base = declarative_base()


class Publisher(Base):
    __tablename__ = "publisher"
    __table_args__ = (UniqueConstraint("name"),)
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    is_active = sa.Column(sa.Boolean, default=False)


class Book(Base):
    __tablename__ = "book"
    __table_args__ = (UniqueConstraint("isbn"),)
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    isbn = sa.Column(sa.String, nullable=False)
    title = sa.Column(sa.String, nullable=False)
    description = sa.Column(sa.String, nullable=True)
    copyright = sa.Column(sa.String, nullable=True)
    publisher_id = sa.Column(
        sa.Integer, sa.ForeignKey(Publisher.id, ondelete="CASCADE")
    )
    publisher = sa.orm.relationship(
        Publisher,
        backref=sa.orm.backref("publishers"),
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

            metadata = sa.MetaData(schema=schema)
            metadata.reflect(engine, views=True)

            book_model = metadata.tables[f"{schema}.book"]
            engine.execute(
                CreateView(
                    schema,
                    "book_view",
                    book_model.select(),
                )
            )

            publisher_model = metadata.tables[f"{schema}.publisher"]
            engine.execute(
                CreateView(
                    schema,
                    "publisher_view",
                    publisher_model.select(),
                )
            )


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
