"""Generic fixtures for PGSync tests."""
import os
import warnings

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import Base, create_database, drop_database
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.sync import Sync
from pgsync.urls import get_postgres_url

warnings.filterwarnings("error")


@pytest.fixture(scope="session")
def base():
    return declarative_base()


@pytest.fixture(scope="session")
def dns():
    return get_postgres_url("testdb")


@pytest.fixture(scope="session")
def engine(dns):
    engine = sa.create_engine(dns)
    drop_database("testdb")
    create_database("testdb")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def connection(engine):
    conn = engine.connect()
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def session(connection):
    Session = sessionmaker(bind=connection, autoflush=True)
    session = Session()
    yield session
    session.close_all()


@pytest.fixture(scope="function")
def sync():
    _sync = Sync(
        {
            "index": "testdb",
            "nodes": {"table": "book"},
        }
    )
    yield _sync
    _sync.logical_slot_get_changes(
        f"{_sync.database}_testdb",
        upto_nchanges=None,
    )
    _sync.engine.connect().close()
    _sync.engine.dispose()
    _sync.session.close()


def pytest_addoption(parser):
    parser.addoption(
        "--repeat",
        action="store",
        help="Number of times to repeat each test",
    )


def pytest_generate_tests(metafunc):
    if metafunc.config.option.repeat is not None:
        count = int(metafunc.config.option.repeat)
        # We're going to duplicate these tests by parametrizing them,
        # which requires that each test has a fixture to accept the parameter.
        # We can add a new fixture like so:
        metafunc.fixturenames.append("tmp_ct")
        # Now we parametrize. This is what happens when we do e.g.,
        # @pytest.mark.parametrize('tmp_ct', range(count))
        # def test_foo(): pass
        metafunc.parametrize("tmp_ct", range(count))


@pytest.fixture(scope="session")
def city_cls(base, country_cls):
    class City(base):
        __tablename__ = "city"
        __table_args__ = (UniqueConstraint("name", "country_id"),)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)
        country_id = sa.Column(sa.Integer, sa.ForeignKey(country_cls.id))
        country = sa.orm.relationship(
            country_cls, backref=sa.orm.backref("countries")
        )

    return City


@pytest.fixture(scope="session")
def country_cls(base, continent_cls):
    class Country(base):
        __tablename__ = "country"
        __table_args__ = (UniqueConstraint("name", "continent_id"),)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)
        continent_id = sa.Column(sa.Integer, sa.ForeignKey(continent_cls.id))
        continent = sa.orm.relationship(
            continent_cls,
            backref=sa.orm.backref("continents"),
        )

    return Country


@pytest.fixture(scope="session")
def continent_cls(base):
    class Continent(base):
        __tablename__ = "continent"
        __table_args__ = (UniqueConstraint("name"),)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)

    return Continent


@pytest.fixture(scope="session")
def publisher_cls(base):
    class Publisher(base):
        __tablename__ = "publisher"
        __table_args__ = (UniqueConstraint("name"),)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)

    return Publisher


@pytest.fixture(scope="session")
def author_cls(base, city_cls):
    class Author(base):
        __tablename__ = "author"
        __table_args__ = (UniqueConstraint("name"),)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)
        birth_year = sa.Column(sa.Integer, nullable=True)
        city_id = sa.Column(sa.Integer, sa.ForeignKey(city_cls.id))
        city = sa.orm.relationship(city_cls, backref=sa.orm.backref("city"))

    return Author


@pytest.fixture(scope="session")
def shelf_cls(base):
    class Shelf(base):
        __tablename__ = "shelf"
        __table_args__ = (UniqueConstraint("shelf"),)
        id = sa.Column(sa.Integer, primary_key=True)
        shelf = sa.Column(sa.String, nullable=False)

    return Shelf


@pytest.fixture(scope="session")
def subject_cls(base):
    class Subject(base):
        __tablename__ = "subject"
        __table_args__ = (UniqueConstraint("name"),)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, nullable=False)

    return Subject


@pytest.fixture(scope="session")
def language_cls(base):
    class Language(base):
        __tablename__ = "language"
        __table_args__ = (UniqueConstraint("code"),)
        id = sa.Column(sa.Integer, primary_key=True)
        code = sa.Column(sa.String, nullable=False)

    return Language


@pytest.fixture(scope="session")
def book_cls(base, publisher_cls):
    class Book(base):
        __tablename__ = "book"
        __table_args__ = (UniqueConstraint("isbn"),)
        isbn = sa.Column(sa.String, primary_key=True)
        title = sa.Column(sa.String, nullable=False)
        description = sa.Column(sa.String, nullable=True)
        copyright = sa.Column(sa.String, nullable=True)
        publisher_id = sa.Column(sa.Integer, sa.ForeignKey(publisher_cls.id))
        publisher = sa.orm.relationship(
            publisher_cls, backref=sa.orm.backref("publishers")
        )

    return Book


@pytest.fixture(scope="session")
def book_author_cls(base, book_cls, author_cls):
    class BookAuthor(base):
        __tablename__ = "book_author"
        __table_args__ = (UniqueConstraint("book_isbn", "author_id"),)
        id = sa.Column(sa.Integer, primary_key=True)
        book_isbn = sa.Column(sa.String, sa.ForeignKey(book_cls.isbn))
        book = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_author_books")
        )
        author_id = sa.Column(sa.Integer, sa.ForeignKey(author_cls.id))
        author = sa.orm.relationship(
            author_cls, backref=sa.orm.backref("authors")
        )

    return BookAuthor


@pytest.fixture(scope="session")
def book_subject_cls(base, book_cls, subject_cls):
    class BookSubject(base):
        __tablename__ = "book_subject"
        __table_args__ = (UniqueConstraint("book_isbn", "subject_id"),)
        id = sa.Column(sa.Integer, primary_key=True)
        book_isbn = sa.Column(sa.String, sa.ForeignKey(book_cls.isbn))
        book = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_subject_books")
        )
        subject_id = sa.Column(sa.Integer, sa.ForeignKey(subject_cls.id))
        subject = sa.orm.relationship(
            subject_cls, backref=sa.orm.backref("subjects")
        )

    return BookSubject


@pytest.fixture(scope="session")
def book_language_cls(base, book_cls, language_cls):
    class BookLanguage(base):
        __tablename__ = "book_language"
        __table_args__ = (UniqueConstraint("book_isbn", "language_id"),)
        id = sa.Column(sa.Integer, primary_key=True)
        book_isbn = sa.Column(sa.String, sa.ForeignKey(book_cls.isbn))
        book = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_language_books")
        )
        language_id = sa.Column(sa.Integer, sa.ForeignKey(language_cls.id))
        language = sa.orm.relationship(
            language_cls, backref=sa.orm.backref("languages")
        )

    return BookLanguage


@pytest.fixture(scope="session")
def book_shelf_cls(base, book_cls, shelf_cls):
    class BookShelf(base):
        __tablename__ = "book_shelf"
        __table_args__ = (UniqueConstraint("book_isbn", "shelf_id"),)
        id = sa.Column(sa.Integer, primary_key=True)
        book_isbn = sa.Column(sa.String, sa.ForeignKey(book_cls.isbn))
        book = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_book_shelf_books")
        )
        shelf_id = sa.Column(sa.Integer, sa.ForeignKey(shelf_cls.id))
        shelf = sa.orm.relationship(
            shelf_cls, backref=sa.orm.backref("shelves")
        )

    return BookShelf


@pytest.fixture(scope="session")
def rating_cls(base, book_cls):
    class Rating(base):
        __tablename__ = "rating"
        __table_args__ = (UniqueConstraint("book_isbn"),)
        id = sa.Column(sa.Integer, primary_key=True)
        book_isbn = sa.Column(sa.String, sa.ForeignKey(book_cls.isbn))
        book = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_rating_books")
        )
        value = sa.Column(sa.Float, nullable=True)

    return Rating


@pytest.fixture(scope="session")
def model_mapping(
    city_cls,
    country_cls,
    continent_cls,
    publisher_cls,
    author_cls,
    shelf_cls,
    subject_cls,
    language_cls,
    book_cls,
    book_author_cls,
    book_subject_cls,
    book_language_cls,
    book_shelf_cls,
    rating_cls,
):
    return {
        "cities": city_cls,
        "countries": country_cls,
        "continents": continent_cls,
        "publishers": publisher_cls,
        "authors": author_cls,
        "shelfs": shelf_cls,
        "subjects": subject_cls,
        "languages": language_cls,
        "books": book_cls,
        "book_authors": book_author_cls,
        "book_subjects": book_subject_cls,
        "book_languages": book_language_cls,
        "book_shelves": book_shelf_cls,
        "ratings": rating_cls,
    }


@pytest.fixture(scope="session")
def table_creator(base, connection, model_mapping):
    sa.orm.configure_mappers()
    base.metadata.create_all(connection)
    pg_base = Base(connection.engine.url.database)
    pg_base.create_triggers(
        connection.engine.url.database,
        DEFAULT_SCHEMA,
    )
    pg_base.drop_replication_slot(f"{connection.engine.url.database}_testdb")
    pg_base.create_replication_slot(f"{connection.engine.url.database}_testdb")
    yield
    pg_base.drop_replication_slot(f"{connection.engine.url.database}_testdb")
    base.metadata.drop_all(connection)
    try:
        os.unlink(f".{connection.engine.url.database}_testdb")
    except OSError:
        pass


@pytest.fixture(scope="session")
def dataset(
    session,
    city_cls,
    country_cls,
    continent_cls,
    publisher_cls,
    author_cls,
    shelf_cls,
    subject_cls,
    language_cls,
    book_cls,
    book_author_cls,
    book_subject_cls,
    book_language_cls,
    book_shelf_cls,
    rating_cls,
):

    eu_continent = continent_cls(name="Europe")
    na_continent = continent_cls(name="North America")
    session.add(eu_continent)
    session.add(na_continent)

    uk_country = country_cls(name="United Kingdom", continent=eu_continent)
    fr_country = country_cls(name="France", continent=eu_continent)
    us_country = country_cls(name="United States", continent=na_continent)
    session.add(uk_country)
    session.add(fr_country)
    session.add(us_country)

    ldn_city = city_cls(name="London", country=uk_country)
    par_city = city_cls(name="Paris", country=fr_country)
    nyc_city = city_cls(name="New York", country=us_country)
    session.add(ldn_city)
    session.add(par_city)
    session.add(nyc_city)

    stephen_king_author = author_cls(
        name="Stephen King",
        birth_year=1970,
        city=ldn_city,
    )
    j_k_rowling_author = author_cls(
        name="J. K. Rowling",
        birth_year=1980,
        city=par_city,
    )
    james_patterson_author = author_cls(
        name="James Patterson",
        birth_year=1990,
        city=nyc_city,
    )
    melinda_leigh_author = author_cls(
        name="Melinda Leigh",
        birth_year=2001,
        city=ldn_city,
    )
    tolu_aina_author = author_cls(
        name="Tolu Aina",
        birth_year=2010,
        city=par_city,
    )
    session.add(stephen_king_author)
    session.add(j_k_rowling_author)
    session.add(james_patterson_author)
    session.add(melinda_leigh_author)
    session.add(tolu_aina_author)

    literature_subject = subject_cls(name="Literature")
    poetry_subject = subject_cls(name="Poetry")
    romance_subject = subject_cls(name="Romance")
    science_subject = subject_cls(name="Science Fiction & Fantasy")
    westerns_subject = subject_cls(name="Westerns")
    session.add(literature_subject)
    session.add(poetry_subject)
    session.add(romance_subject)
    session.add(science_subject)
    session.add(westerns_subject)

    en_languages = language_cls(code="EN")
    fr_languages = language_cls(code="FR")
    session.add(en_languages)
    session.add(fr_languages)

    shelf_a_book_shelf = book_shelf_cls(shelf="Shelf A")
    shelf_b_book_shelf = book_shelf_cls(shelf="Shelf B")
    session.add(shelf_a_book_shelf)
    session.add(shelf_b_book_shelf)

    book_001 = book_cls(
        isbn="001",
        title="It",
        description="Stephens Kings It",
        publisher=publisher_cls(name="Oxford Press"),
    )
    book_002 = book_cls(
        isbn="002",
        title="The Body",
        description="Lodsdcsdrem ipsum dodscdslor sit amet",
        publisher=publisher_cls(name="Oxford Press"),
    )
    book_003 = book_cls(
        isbn="003",
        title="Harry Potter and the Sorcerer's Stone",
        description="Harry Potter has never been",
        publisher=publisher_cls(name="Penguin Books"),
    )
    book_004 = book_cls(
        isbn="004",
        title="Harry Potter and the Chamber of Secrets",
        description="The Dursleys were so mean and hideous that summer that "
        "all Harry Potter wanted was to get back to the Hogwarts "
        "School for Witchcraft and Wizardry",
        publisher=publisher_cls(name="Penguin Books"),
    )
    book_005 = book_cls(
        isbn="005",
        title="The 17th Suspect",
        description="A series of shootings exposes San Francisco to a "
        "methodical yet unpredictable killer, and a reluctant "
        "woman decides to put her trust in Sergeant Lindsay Boxer",
        publisher=publisher_cls(name="Pearson Press"),
    )
    book_006 = book_cls(
        isbn="006",
        title="The President Is Missing",
        description="The publishing event of 2018: Bill Clinton and James "
        "Patterson's The President Is Missing is a superlative "
        "thriller",
        publisher=publisher_cls(name="Pearson Press"),
    )
    book_007 = book_cls(
        isbn="007",
        title="Say You're Sorry",
        description="deserunt mollit anim id est laborum",
        publisher=publisher_cls(name="Reutgers Press"),
    )
    book_008 = book_cls(
        isbn="008",
        title="Bones Don't Lie",
        description="Lorem ipsum",
        publisher=publisher_cls(name="Reutgers Press"),
    )
    session.add(book_001)
    session.add(book_002)
    session.add(book_003)
    session.add(book_004)
    session.add(book_005)
    session.add(book_006)
    session.add(book_007)
    session.add(book_008)

    book_authors = [
        book_author_cls(
            book=book_001,
            author=stephen_king_author,
        ),
        book_author_cls(
            book=book_002,
            author=stephen_king_author,
        ),
        book_author_cls(
            book=book_003,
            author=j_k_rowling_author,
        ),
        book_author_cls(
            book=book_004,
            author=j_k_rowling_author,
        ),
        book_author_cls(
            book=book_005,
            author=james_patterson_author,
        ),
        book_author_cls(
            book=book_006,
            author=james_patterson_author,
        ),
        book_author_cls(
            book=book_007,
            author=melinda_leigh_author,
        ),
        book_author_cls(
            book=book_008,
            author=melinda_leigh_author,
        ),
        book_author_cls(
            book=book_007,
            author=tolu_aina_author,
        ),
        book_author_cls(
            book=book_008,
            author=tolu_aina_author,
        ),
    ]
    session.add_all(book_authors)

    book_subjects = [
        book_subject_cls(book=book_001, subject=literature_subject),
        book_subject_cls(book=book_002, subject=literature_subject),
        book_subject_cls(book=book_003, subject=poetry_subject),
        book_subject_cls(book=book_004, subject=poetry_subject),
        book_subject_cls(book=book_005, subject=romance_subject),
        book_subject_cls(book=book_006, subject=romance_subject),
        book_subject_cls(book=book_007, subject=science_subject),
        book_subject_cls(book=book_008, subject=science_subject),
    ]
    session.add_all(book_subjects)

    book_languages = [
        book_language_cls(book=book_001, language=en_languages),
        book_language_cls(book=book_002, language=en_languages),
        book_language_cls(book=book_003, language=en_languages),
        book_language_cls(book=book_004, language=en_languages),
        book_language_cls(book=book_005, language=en_languages),
        book_language_cls(book=book_006, language=en_languages),
        book_language_cls(book=book_007, language=en_languages),
        book_language_cls(book=book_008, language=en_languages),
        book_language_cls(book=book_001, language=fr_languages),
        book_language_cls(book=book_002, language=fr_languages),
        book_language_cls(book=book_003, language=fr_languages),
        book_language_cls(book=book_004, language=fr_languages),
        book_language_cls(book=book_005, language=fr_languages),
        book_language_cls(book=book_006, language=fr_languages),
        book_language_cls(book=book_007, language=fr_languages),
        book_language_cls(book=book_008, language=fr_languages),
    ]
    session.add_all(book_languages)

    book_shelves = [
        book_shelf_cls(book=book_001, shelf=shelf_a_book_shelf),
        book_shelf_cls(book=book_002, shelf=shelf_b_book_shelf),
        book_shelf_cls(book=book_003, shelf=shelf_a_book_shelf),
        book_shelf_cls(book=book_004, shelf=shelf_b_book_shelf),
        book_shelf_cls(book=book_005, shelf=shelf_a_book_shelf),
        book_shelf_cls(book=book_006, shelf=shelf_b_book_shelf),
        book_shelf_cls(book=book_007, shelf=shelf_a_book_shelf),
        book_shelf_cls(book=book_008, shelf=shelf_b_book_shelf),
    ]
    session.add_all(book_shelves)

    ratings = [
        rating_cls(book=book_001, rating=1),
        rating_cls(book=book_002, rating=2),
        rating_cls(book=book_003, rating=3),
        rating_cls(book=book_004, rating=4),
        rating_cls(book=book_005, rating=5),
        rating_cls(book=book_006, rating=6),
        rating_cls(book=book_007, rating=7),
        rating_cls(book=book_008, rating=8),
    ]
    session.add_all(ratings)

    session.commit()
