from datetime import datetime

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


class Continent(Base):
    __tablename__ = "continent"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class Country(Base):
    __tablename__ = "country"
    __table_args__ = (UniqueConstraint("name", "continent_id"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    continent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Continent.id, ondelete="CASCADE")
    )
    continent: Mapped[Continent] = sa.orm.relationship(
        Continent, backref=sa.orm.backref("continents")
    )


class City(Base):
    __tablename__ = "city"
    __table_args__ = (UniqueConstraint("name", "country_id"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    country_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Country.id, ondelete="CASCADE"),
    )
    country: Mapped[Country] = sa.orm.relationship(
        Country,
        backref=sa.orm.backref("countries"),
    )


class Publisher(Base):
    __tablename__ = "publisher"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=False)


class Author(Base):
    __tablename__ = "author"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    date_of_birth: Mapped[datetime] = mapped_column(sa.DateTime, nullable=True)
    city_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(City.id, ondelete="CASCADE")
    )
    city: Mapped[City] = sa.orm.relationship(
        City,
        backref=sa.orm.backref("city"),
    )


class Shelf(Base):
    __tablename__ = "shelf"
    __table_args__ = (UniqueConstraint("shelf"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    shelf: Mapped[str] = mapped_column(sa.String, nullable=False)


class Subject(Base):
    __tablename__ = "subject"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class Language(Base):
    __tablename__ = "language"
    __table_args__ = (UniqueConstraint("code"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    code: Mapped[str] = mapped_column(sa.String, nullable=False)


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
    tags: Mapped[dict] = mapped_column(
        sa.dialects.postgresql.JSONB, nullable=True
    )
    doc: Mapped[dict] = mapped_column(
        sa.dialects.postgresql.JSONB, nullable=True
    )
    publisher_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Publisher.id, ondelete="CASCADE")
    )
    publisher: Mapped[Publisher] = sa.orm.relationship(
        Publisher,
        backref=sa.orm.backref("publishers"),
    )
    publish_date: Mapped[datetime] = mapped_column(sa.DateTime, nullable=True)


class Rating(Base):
    __tablename__ = "rating"
    __table_args__ = (UniqueConstraint("book_isbn"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    book_isbn: Mapped[str] = mapped_column(
        sa.String, sa.ForeignKey(Book.isbn, ondelete="CASCADE")
    )
    book: Mapped[Book] = sa.orm.relationship(
        Book, backref=sa.orm.backref("ratings")
    )
    value: Mapped[float] = mapped_column(sa.Float, nullable=True)


class BookAuthor(Base):
    __tablename__ = "book_author"
    __table_args__ = (UniqueConstraint("book_isbn", "author_id"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    book_isbn: Mapped[str] = mapped_column(
        sa.String, sa.ForeignKey(Book.isbn, ondelete="CASCADE")
    )
    book: Mapped[Book] = sa.orm.relationship(
        Book,
        backref=sa.orm.backref("book_author_books"),
    )
    author_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Author.id, ondelete="CASCADE")
    )
    author: Mapped[Author] = sa.orm.relationship(
        Author,
        backref=sa.orm.backref("authors"),
    )


class BookSubject(Base):
    __tablename__ = "book_subject"
    __table_args__ = (UniqueConstraint("book_isbn", "subject_id"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    book_isbn: Mapped[str] = mapped_column(
        sa.String, sa.ForeignKey(Book.isbn, ondelete="CASCADE")
    )
    book: Mapped[Book] = sa.orm.relationship(
        Book,
        backref=sa.orm.backref("book_subject_books"),
    )
    subject_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Subject.id, ondelete="CASCADE")
    )
    subject: Mapped[Subject] = sa.orm.relationship(
        Subject,
        backref=sa.orm.backref("subjects"),
    )


class BookLanguage(Base):
    __tablename__ = "book_language"
    __table_args__ = (UniqueConstraint("book_isbn", "language_id"),)

    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    book_isbn: Mapped[str] = mapped_column(
        sa.String, sa.ForeignKey(Book.isbn, ondelete="CASCADE")
    )
    book: Mapped[Book] = sa.orm.relationship(
        Book,
        backref=sa.orm.backref("book_language_books"),
    )
    language_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Language.id, ondelete="CASCADE")
    )
    language: Mapped[Language] = sa.orm.relationship(
        Language,
        backref=sa.orm.backref("languages"),
    )


class BookShelf(Base):
    __tablename__ = "bookshelf"
    __table_args__ = (UniqueConstraint("book_isbn", "shelf_id"),)

    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    book_isbn: Mapped[str] = mapped_column(
        sa.String, sa.ForeignKey(Book.isbn, ondelete="CASCADE")
    )
    book: Mapped[Book] = sa.orm.relationship(
        Book,
        backref=sa.orm.backref("book_bookshelf_books"),
    )
    shelf_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Shelf.id, ondelete="CASCADE")
    )
    shelf: Mapped[Shelf] = sa.orm.relationship(
        Shelf, backref=sa.orm.backref("shelves")
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
