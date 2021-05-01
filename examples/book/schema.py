import json

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import create_database, create_schema, pg_engine
from pgsync.constants import SCHEMA
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


class Continent(Base):
    __tablename__ = 'continent'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)


class Country(Base):
    __tablename__ = 'country'
    __table_args__ = (
        UniqueConstraint('name', 'continent_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    continent_id = sa.Column(
        sa.Integer, sa.ForeignKey(Continent.id)
    )
    continent = sa.orm.relationship(
        Continent,
        backref=sa.orm.backref('continents')
    )


class City(Base):
    __tablename__ = 'city'
    __table_args__ = (
        UniqueConstraint('name', 'country_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    country_id = sa.Column(
        sa.Integer,
        sa.ForeignKey(Country.id),
    )
    country = sa.orm.relationship(
        Country,
        backref=sa.orm.backref('countries'),
    )


class Publisher(Base):
    __tablename__ = 'publisher'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    is_active = sa.Column(sa.Boolean, default=False)


class Author(Base):
    __tablename__ = 'author'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)
    date_of_birth = sa.Column(sa.DateTime, nullable=True)
    city_id = sa.Column(sa.Integer, sa.ForeignKey(City.id))
    city = sa.orm.relationship(
        City,
        backref=sa.orm.backref('city'),
    )


class Shelf(Base):
    __tablename__ = 'shelf'
    __table_args__ = (
        UniqueConstraint('shelf'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    shelf = sa.Column(sa.String, nullable=False)


class Subject(Base):
    __tablename__ = 'subject'
    __table_args__ = (
        UniqueConstraint('name'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name = sa.Column(sa.String, nullable=False)


class Language(Base):
    __tablename__ = 'language'
    __table_args__ = (
        UniqueConstraint('code'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    code = sa.Column(sa.String, nullable=False)


class Book(Base):
    __tablename__ = 'book'
    __table_args__ = (
        UniqueConstraint('isbn'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    isbn = sa.Column(sa.String, nullable=False)
    title = sa.Column(sa.String, nullable=False)
    description = sa.Column(sa.String, nullable=True)
    copyright = sa.Column(sa.String, nullable=True)
    tags = sa.Column(sa.dialects.postgresql.JSONB, nullable=True)
    doc = sa.Column(sa.dialects.postgresql.JSONB, nullable=True)
    publisher_id = sa.Column(
        sa.Integer, sa.ForeignKey(Publisher.id)
    )
    publisher = sa.orm.relationship(
        Publisher,
        backref=sa.orm.backref('publishers'),
    )


class Rating(Base):
    __tablename__ = 'rating'
    __table_args__ = (
        UniqueConstraint('book_isbn'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    book_isbn = sa.Column(
        sa.String, sa.ForeignKey(Book.isbn)
    )
    book = sa.orm.relationship(
        Book,
        backref=sa.orm.backref('ratings')
    )
    value = sa.Column(sa.Float, nullable=True)


class BookAuthor(Base):
    __tablename__ = 'book_author'
    __table_args__ = (
        UniqueConstraint('book_isbn', 'author_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    book_isbn = sa.Column(sa.String, sa.ForeignKey(Book.isbn))
    book = sa.orm.relationship(
        Book,
        backref=sa.orm.backref('book_author_books'),
    )
    author_id = sa.Column(
        sa.Integer, sa.ForeignKey(Author.id)
    )
    author = sa.orm.relationship(
        Author,
        backref=sa.orm.backref('authors'),
    )


class BookSubject(Base):
    __tablename__ = 'book_subject'
    __table_args__ = (
        UniqueConstraint('book_isbn', 'subject_id'),
    )
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    book_isbn = sa.Column(
        sa.String, sa.ForeignKey(Book.isbn)
    )
    book = sa.orm.relationship(
        Book,
        backref=sa.orm.backref('book_subject_books'),
    )
    subject_id = sa.Column(
        sa.Integer, sa.ForeignKey(Subject.id)
    )
    subject = sa.orm.relationship(
        Subject,
        backref=sa.orm.backref('subjects'),
    )


class BookLanguage(Base):
    __tablename__ = 'book_language'
    __table_args__ = (
        UniqueConstraint('book_isbn', 'language_id'),
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    book_isbn = sa.Column(sa.String, sa.ForeignKey(Book.isbn))
    book = sa.orm.relationship(
        Book,
        backref=sa.orm.backref('book_language_books'),
    )
    language_id = sa.Column(sa.Integer, sa.ForeignKey(Language.id))
    language = sa.orm.relationship(
        Language,
        backref=sa.orm.backref('languages'),
    )


class BookShelf(Base):
    __tablename__ = 'bookshelf'
    __table_args__ = (
        UniqueConstraint('book_isbn', 'shelf_id'),
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    book_isbn = sa.Column(sa.String, sa.ForeignKey(Book.isbn))
    book = sa.orm.relationship(
        Book,
        backref=sa.orm.backref('book_bookshelf_books'),
    )
    shelf_id = sa.Column(sa.Integer, sa.ForeignKey(Shelf.id))
    shelf = sa.orm.relationship(Shelf, backref=sa.orm.backref('shelves'))


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get('database', document['index'])
        schema = document.get('schema', SCHEMA)
        create_database(database)
        engine = pg_engine(database=database)
        create_schema(engine, schema)
        engine = engine.connect().execution_options(
            schema_translate_map={
                None: schema
            }
        )
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == '__main__':
    main()
