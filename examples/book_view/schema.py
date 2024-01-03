import click
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
from pgsync.utils import config_loader, get_config
from pgsync.view import CreateView


class Base(DeclarativeBase):
    pass


class Publisher(Base):
    __tablename__ = "publisher"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=False)


class Book(Base):
    __tablename__ = "book"
    __table_args__ = (UniqueConstraint("isbn"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    isbn: Mapped[str] = mapped_column(sa.String, nullable=False)
    title: Mapped[str] = mapped_column(sa.String, nullable=False)
    description: Mapped[str] = mapped_column(sa.String, nullable=True)
    copyright: Mapped[str] = mapped_column(sa.String, nullable=True)
    publisher_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Publisher.id, ondelete="CASCADE")
    )
    publisher: Mapped[Publisher] = sa.orm.relationship(
        Publisher,
        backref=sa.orm.backref("publishers"),
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

            metadata = sa.MetaData(schema=schema)
            metadata.reflect(engine, views=True)

            book_model = metadata.tables[f"{schema}.book"]

            with engine.connect() as conn:
                conn.execute(
                    CreateView(
                        schema,
                        "book_view",
                        book_model.select(),
                    )
                )
                conn.commit()

            publisher_model = metadata.tables[f"{schema}.publisher"]

            with engine.connect() as conn:
                conn.execute(
                    CreateView(
                        schema,
                        "publisher_view",
                        publisher_model.select(),
                    )
                )
                conn.commit()


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
