"""Generic fixtures for PGSync tests."""

import logging
import os

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from sqlalchemy.schema import UniqueConstraint

from pgsync.base import Base, create_database, drop_database
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.singleton import Singleton
from pgsync.sync import Sync
from pgsync.urls import get_postgres_url

logging.getLogger("faker").setLevel(logging.ERROR)


@pytest.fixture(scope="session")
def base():
    class Base(DeclarativeBase):
        pass

    return Base


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
            "database": "testdb",
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
    Singleton._instances = {}


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
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)
        country_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(country_cls.id)
        )
        country: Mapped[country_cls] = sa.orm.relationship(
            country_cls, backref=sa.orm.backref("countries")
        )

    return City


@pytest.fixture(scope="session")
def country_cls(base, continent_cls):
    class Country(base):
        __tablename__ = "country"
        __table_args__ = (UniqueConstraint("name", "continent_id"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)
        continent_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(continent_cls.id)
        )
        continent: Mapped[continent_cls] = sa.orm.relationship(
            continent_cls,
            backref=sa.orm.backref("continents"),
        )

    return Country


@pytest.fixture(scope="session")
def continent_cls(base):
    class Continent(base):
        __tablename__ = "continent"
        __table_args__ = (UniqueConstraint("name"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Continent


@pytest.fixture(scope="session")
def publisher_cls(base):
    class Publisher(base):
        __tablename__ = "publisher"
        __table_args__ = (UniqueConstraint("name"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Publisher


@pytest.fixture(scope="session")
def author_cls(base, city_cls):
    class Author(base):
        __tablename__ = "author"
        __table_args__ = (UniqueConstraint("name"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)
        birth_year: Mapped[int] = mapped_column(sa.Integer, nullable=True)
        city_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(city_cls.id)
        )
        city: Mapped[city_cls] = sa.orm.relationship(
            city_cls, backref=sa.orm.backref("city")
        )

    return Author


@pytest.fixture(scope="session")
def shelf_cls(base):
    class Shelf(base):
        __tablename__ = "shelf"
        __table_args__ = (UniqueConstraint("shelf"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        shelf: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Shelf


@pytest.fixture(scope="session")
def subject_cls(base):
    class Subject(base):
        __tablename__ = "subject"
        __table_args__ = (UniqueConstraint("name"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Subject


@pytest.fixture(scope="session")
def language_cls(base):
    class Language(base):
        __tablename__ = "language"
        __table_args__ = (UniqueConstraint("code"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        code: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Language


@pytest.fixture(scope="session")
def contact_cls(base):
    class Contact(base):
        __tablename__ = "contact"
        __table_args__ = (UniqueConstraint("name"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Contact


@pytest.fixture(scope="session")
def contact_item_cls(base, contact_cls):
    class ContactItem(base):
        __tablename__ = "contact_item"
        __table_args__ = (
            UniqueConstraint("name"),
            UniqueConstraint("contact_id"),
        )
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        name: Mapped[str] = mapped_column(sa.String, nullable=False)
        contact_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(contact_cls.id)
        )
        contact: Mapped[contact_cls] = sa.orm.relationship(
            contact_cls, backref=sa.orm.backref("contacts")
        )

    return ContactItem


@pytest.fixture(scope="session")
def user_cls(base, contact_cls):
    class User(base):
        __tablename__ = "user"
        __table_args__ = (
            UniqueConstraint("name"),
            UniqueConstraint("contact_id"),
        )
        id: Mapped[int] = mapped_column(
            sa.Integer, primary_key=True, autoincrement=True
        )
        name: Mapped[str] = mapped_column(sa.String, nullable=False)
        contact_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(contact_cls.id)
        )
        contact: Mapped[contact_cls] = sa.orm.relationship(
            contact_cls, backref=sa.orm.backref("user_contacts")
        )

    return User


@pytest.fixture(scope="session")
def book_cls(base, publisher_cls, user_cls):
    class Book(base):
        __tablename__ = "book"
        __table_args__ = (UniqueConstraint("isbn"),)
        isbn: Mapped[str] = mapped_column(sa.String, primary_key=True)
        title: Mapped[str] = mapped_column(sa.String, nullable=False)
        description: Mapped[str] = mapped_column(sa.String, nullable=True)
        copyright: Mapped[str] = mapped_column(sa.String, nullable=True)
        publisher_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(publisher_cls.id), nullable=True
        )
        publisher: Mapped[publisher_cls] = sa.orm.relationship(
            publisher_cls, backref=sa.orm.backref("publishers")
        )
        buyer_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(user_cls.id), nullable=True
        )
        buyer: Mapped[user_cls] = sa.orm.relationship(
            user_cls,
            backref=sa.orm.backref("buyers"),
            foreign_keys=[buyer_id],
        )
        seller_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(user_cls.id), nullable=True
        )
        seller: Mapped[user_cls] = sa.orm.relationship(
            user_cls,
            backref=sa.orm.backref("sellers"),
            foreign_keys=[seller_id],
        )
        tags: Mapped[sa.dialects.postgresql.JSONB] = mapped_column(
            sa.dialects.postgresql.JSONB, nullable=True
        )

    return Book


@pytest.fixture(scope="session")
def book_author_cls(base, book_cls, author_cls):
    class BookAuthor(base):
        __tablename__ = "book_author"
        __table_args__ = (UniqueConstraint("book_isbn", "author_id"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        book_isbn: Mapped[str] = mapped_column(
            sa.String, sa.ForeignKey(book_cls.isbn)
        )
        book: Mapped[book_cls] = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_author_books")
        )
        author_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(author_cls.id)
        )
        author: Mapped[author_cls] = sa.orm.relationship(
            author_cls, backref=sa.orm.backref("authors")
        )

    return BookAuthor


@pytest.fixture(scope="session")
def book_subject_cls(base, book_cls, subject_cls):
    class BookSubject(base):
        __tablename__ = "book_subject"
        __table_args__ = (UniqueConstraint("book_isbn", "subject_id"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        book_isbn: Mapped[str] = mapped_column(
            sa.String, sa.ForeignKey(book_cls.isbn)
        )
        book: Mapped[book_cls] = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_subject_books")
        )
        subject_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(subject_cls.id)
        )
        subject: Mapped[subject_cls] = sa.orm.relationship(
            subject_cls, backref=sa.orm.backref("subjects")
        )

    return BookSubject


@pytest.fixture(scope="session")
def book_language_cls(base, book_cls, language_cls):
    class BookLanguage(base):
        __tablename__ = "book_language"
        __table_args__ = (UniqueConstraint("book_isbn", "language_id"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        book_isbn: Mapped[str] = mapped_column(
            sa.String, sa.ForeignKey(book_cls.isbn)
        )
        book: Mapped[book_cls] = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_language_books")
        )
        language_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(language_cls.id)
        )
        language: Mapped[language_cls] = sa.orm.relationship(
            language_cls, backref=sa.orm.backref("languages")
        )

    return BookLanguage


@pytest.fixture(scope="session")
def book_shelf_cls(base, book_cls, shelf_cls):
    class BookShelf(base):
        __tablename__ = "book_shelf"
        __table_args__ = (UniqueConstraint("book_isbn", "shelf_id"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        book_isbn: Mapped[str] = mapped_column(
            sa.String, sa.ForeignKey(book_cls.isbn)
        )
        book: Mapped[book_cls] = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_book_shelf_books")
        )
        shelf_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(shelf_cls.id)
        )
        shelf: Mapped[shelf_cls] = sa.orm.relationship(
            shelf_cls, backref=sa.orm.backref("shelves")
        )

    return BookShelf


@pytest.fixture(scope="session")
def rating_cls(base, book_cls):
    class Rating(base):
        __tablename__ = "rating"
        __table_args__ = (UniqueConstraint("book_isbn"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        book_isbn: Mapped[str] = mapped_column(
            sa.String, sa.ForeignKey(book_cls.isbn)
        )
        book: Mapped[book_cls] = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_rating_books")
        )
        value: Mapped[float] = mapped_column(sa.Float, nullable=True)

    return Rating


@pytest.fixture(scope="session")
def group_cls(base):
    class Group(base):
        __tablename__ = "group"
        __table_args__ = (UniqueConstraint("group_name"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        group_name: Mapped[str] = mapped_column(sa.String, nullable=False)

    return Group


@pytest.fixture(scope="session")
def book_group_cls(base, book_cls, group_cls):
    class BookGroup(base):
        __tablename__ = "book_group"
        __table_args__ = (UniqueConstraint("book_isbn", "group_id"),)
        id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
        book_isbn: Mapped[str] = mapped_column(
            sa.String, sa.ForeignKey(book_cls.isbn)
        )
        book: Mapped[book_cls] = sa.orm.relationship(
            book_cls, backref=sa.orm.backref("book_book_group_books")
        )
        group_id: Mapped[int] = mapped_column(
            sa.Integer, sa.ForeignKey(group_cls.id)
        )
        group: Mapped[group_cls] = sa.orm.relationship(
            group_cls, backref=sa.orm.backref("groups"), cascade="all,delete"
        )

    return BookGroup


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
    user_cls,
    contact_cls,
    contact_item_cls,
    book_group_cls,
    group_cls,
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
        "users": user_cls,
        "contacts": contact_cls,
        "contact_items": contact_item_cls,
        "book_groups": book_group_cls,
        "groups": group_cls,
    }


@pytest.fixture(scope="session")
def table_creator(base, connection, model_mapping):
    sa.orm.configure_mappers()
    with connection.engine.connect() as conn:
        base.metadata.create_all(connection.engine)
        conn.commit()
    pg_base = Base(connection.engine.url.database)
    pg_base.create_triggers(
        connection.engine.url.database,
        DEFAULT_SCHEMA,
    )
    pg_base.drop_replication_slot(f"{connection.engine.url.database}_testdb")
    pg_base.create_replication_slot(f"{connection.engine.url.database}_testdb")
    yield
    pg_base.drop_replication_slot(f"{connection.engine.url.database}_testdb")
    with connection.engine.connect() as conn:
        base.metadata.drop_all(connection.engine)
        conn.commit()
    try:
        os.unlink(f".{connection.engine.url.database}_testdb")
    except (OSError, FileNotFoundError):
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
    book_group_cls,
    group_cls,
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
        tags=["a", "b", 1],
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
