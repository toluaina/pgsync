"""Sample data for the book example."""

import datetime
import random
import typing as t

from faker import Faker
from sqlalchemy.orm import Session

from .models import (
    Author,
    Book,
    BookAuthor,
    BookLanguage,
    BookShelf,
    BookSubject,
    City,
    Continent,
    Country,
    Language,
    Publisher,
    Rating,
    Shelf,
    Subject,
)


def seed(session: Session, size: int = 0) -> None:
    """Add the sample books, then ``size`` random books."""
    continents: t.Dict[str, Continent] = {
        "Europe": Continent(name="Europe"),
        "North America": Continent(name="North America"),
    }
    session.add_all(continents.values())
    session.flush()
    countries: t.Dict[str, Country] = {
        "United Kingdom": Country(
            name="United Kingdom",
            continent=continents["Europe"],
        ),
        "France": Country(
            name="France",
            continent=continents["Europe"],
        ),
        "United States": Country(
            name="United States",
            continent=continents["North America"],
        ),
    }
    session.add_all(countries.values())
    session.flush()
    cities: t.Dict[str, City] = {
        "London": City(name="London", country=countries["United Kingdom"]),
        "Paris": City(name="Paris", country=countries["France"]),
        "New York": City(name="New York", country=countries["United States"]),
    }
    session.add_all(cities.values())
    session.flush()
    publishers: t.Dict[str, Publisher] = {
        "Oxford Press": Publisher(name="Oxford Press", is_active=True),
        "Penguin Books": Publisher(name="Penguin Books", is_active=False),
        "Pearson Press": Publisher(name="Pearson Press", is_active=True),
        "Reutgers Press": Publisher(name="Reutgers Press", is_active=False),
    }
    session.add_all(publishers.values())
    session.flush()
    authors: t.Dict[str, Author] = {
        "Stephen King": Author(
            name="Stephen King",
            date_of_birth=datetime.datetime(1947, 9, 21),
            city=cities["London"],
        ),
        "J. K. Rowling": Author(
            name="J. K. Rowling",
            date_of_birth=datetime.datetime(1965, 7, 31),
            city=cities["Paris"],
        ),
        "James Patterson": Author(
            name="James Patterson",
            date_of_birth=datetime.datetime(1947, 3, 22),
            city=cities["New York"],
        ),
        "Melinda Leigh": Author(
            name="Melinda Leigh",
            date_of_birth=datetime.datetime(1980, 1, 1),
            city=cities["Paris"],
        ),
        "Tolu Aina": Author(
            name="Tolu Aina",
            date_of_birth=datetime.datetime(1980, 5, 21),
            city=cities["London"],
        ),
    }
    session.add_all(authors.values())
    session.flush()
    subjects: t.Dict[str, Subject] = {
        "Literature": Subject(name="Literature"),
        "Poetry": Subject(name="Poetry"),
        "Romance": Subject(name="Romance"),
        "Science Fiction & Fantasy": Subject(name="Science Fiction & Fantasy"),
        "Westerns": Subject(name="Westerns"),
    }
    session.add_all(subjects.values())
    session.flush()
    languages: t.Dict[str, Language] = {
        "en-GB": Language(code="en-GB"),
        "en-US": Language(code="en-US"),
        "de-DE": Language(code="de-DE"),
        "af-ZA": Language(code="af-ZA"),
        "es-ES": Language(code="es-ES"),
        "fr-FR": Language(code="fr-FR"),
        "it-IT": Language(code="it-IT"),
        "ja-JP": Language(code="ja-JP"),
    }
    session.add_all(languages.values())
    session.flush()
    shelves: t.Dict[str, Shelf] = {
        "Shelf A": Shelf(shelf="Shelf A"),
        "Shelf B": Shelf(shelf="Shelf B"),
    }
    session.add_all(shelves.values())
    session.flush()
    books: t.Dict[str, Book] = {
        "001": Book(
            isbn="001",
            title="It",
            description="Stephens Kings It",
            publisher=publishers["Oxford Press"],
            tags=["a", "b", "c"],
            doc={
                "i": 73,
                "bool": True,
                "firstname": "Glenda",
                "lastname": "Judye",
                "nick_names": [
                    "Beatriz",
                    "Jean",
                    "Carilyn",
                    "Carol-Jean",
                    "Sara-Ann",
                ],
                "coordinates": {"lat": 21.1, "lon": 32.9},
                "a": {"b": {"c": [0, 1, 2, 3, 4]}},
                "x": [{"y": 0, "z": 5}, {"y": 1, "z": 6}],
                "generation": {"name": "X"},
            },
            publish_date=datetime.datetime(1980, 1, 1),
        ),
        "002": Book(
            isbn="002",
            title="The Body",
            description="Lodsdcsdrem ipsum dodscdslor sit amet",
            publisher=publishers["Oxford Press"],
            tags=["d", "e", "f"],
            doc={
                "i": 99,
                "bool": False,
                "firstname": "Jack",
                "lastname": "Jones",
                "nick_names": [
                    "Jack",
                    "Jones",
                    "Jay",
                    "Jay-Jay",
                    "Jackie",
                ],
                "coordinates": {"lat": 25.1, "lon": 52.2},
                "a": {"b": {"c": [2, 3, 4, 5, 6]}},
                "x": [{"y": 2, "z": 3}, {"y": 7, "z": 2}],
                "generation": {"name": "X"},
            },
            # publish_date="infinity",
        ),
        "003": Book(
            isbn="003",
            title="Harry Potter and the Sorcerer's Stone",
            description="Harry Potter has never been",
            publisher=publishers["Penguin Books"],
            tags=["g", "h", "i"],
            doc={
                "i": 13,
                "bool": True,
                "firstname": "Mary",
                "lastname": "Jane",
                "nick_names": [
                    "Mariane",
                    "May",
                    "Maey",
                    "M-Jane",
                    "Jane",
                ],
                "coordinates": {"lat": 24.9, "lon": 93.2},
                "a": {"b": {"c": [9, 8, 7, 6, 5]}},
                "x": [{"y": 3, "z": 5}, {"y": 8, "z": 2}],
                "generation": {"name": "X"},
            },
            # publish_date="-infinity",
        ),
        "004": Book(
            isbn="004",
            title="Harry Potter and the Chamber of Secrets",
            description="The Dursleys were so mean and hideous that summer "
            "that all Harry Potter wanted was to get back to the "
            "Hogwarts School for Witchcraft and Wizardry",
            publisher=publishers["Penguin Books"],
            tags=["j", "k", "l"],
            doc={
                "i": 43,
                "bool": False,
                "firstname": "Kermit",
                "lastname": "Frog",
                "nick_names": ["Kermit", "Frog", "Ker", "Boss", "K"],
                "coordinates": {"lat": 22.7, "lon": 35.2},
                "a": {"b": {"c": [9, 1, 2, 8, 4]}},
                "x": [{"y": 3, "z": 2}, {"y": 9, "z": 2}],
                "generation": {"name": "Z"},
            },
        ),
        "005": Book(
            isbn="005",
            title="The 17th Suspect",
            description="A series of shootings exposes San Francisco to a "
            "methodical yet unpredictable killer, and a reluctant "
            "woman decides to put her trust in Sergeant Lindsay "
            "Boxer",
            publisher=publishers["Pearson Press"],
            tags=["m", "n", "o"],
        ),
        "006": Book(
            isbn="006",
            title="The President Is Missing",
            description="The publishing event of 2018: Bill Clinton and James "
            "Patterson's The President Is Missing is a "
            "superlative thriller",
            publisher=publishers["Pearson Press"],
        ),
        "007": Book(
            isbn="007",
            title="Say You're Sorry",
            description="deserunt mollit anim id est laborum",
            publisher=publishers["Reutgers Press"],
        ),
        "008": Book(
            isbn="008",
            title="Bones Don't Lie",
            description="Lorem ipsum",
            publisher=publishers["Reutgers Press"],
        ),
    }
    session.add_all(books.values())
    session.flush()
    ratings: t.List[Rating] = [
        Rating(value=1.1, book=books["001"]),
        Rating(value=2.1, book=books["002"]),
        Rating(value=3.1, book=books["003"]),
        Rating(value=4.1, book=books["004"]),
        Rating(value=5.1, book=books["005"]),
        Rating(value=6.1, book=books["006"]),
        Rating(value=7.1, book=books["007"]),
        Rating(value=8.1, book=books["008"]),
    ]
    session.add_all(ratings)
    session.flush()
    book_authors: t.List[BookAuthor] = [
        BookAuthor(book=books["001"], author=authors["Stephen King"]),
        BookAuthor(book=books["002"], author=authors["Stephen King"]),
        BookAuthor(book=books["003"], author=authors["J. K. Rowling"]),
        BookAuthor(book=books["004"], author=authors["J. K. Rowling"]),
        BookAuthor(book=books["005"], author=authors["James Patterson"]),
        BookAuthor(book=books["006"], author=authors["James Patterson"]),
        BookAuthor(book=books["007"], author=authors["Melinda Leigh"]),
        BookAuthor(book=books["008"], author=authors["Melinda Leigh"]),
        BookAuthor(book=books["007"], author=authors["Tolu Aina"]),
        BookAuthor(book=books["008"], author=authors["Tolu Aina"]),
    ]
    session.add_all(book_authors)
    session.flush()
    book_subjects: t.List[BookSubject] = [
        BookSubject(book=books["001"], subject=subjects["Literature"]),
        BookSubject(book=books["002"], subject=subjects["Literature"]),
        BookSubject(book=books["003"], subject=subjects["Poetry"]),
        BookSubject(book=books["004"], subject=subjects["Poetry"]),
        BookSubject(book=books["005"], subject=subjects["Romance"]),
        BookSubject(book=books["006"], subject=subjects["Romance"]),
        BookSubject(
            book=books["007"],
            subject=subjects["Science Fiction & Fantasy"],
        ),
        BookSubject(
            book=books["008"],
            subject=subjects["Science Fiction & Fantasy"],
        ),
    ]
    session.add_all(book_subjects)
    session.flush()
    book_languages: t.List[BookLanguage] = [
        BookLanguage(book=books["001"], language=languages["en-GB"]),
        BookLanguage(book=books["002"], language=languages["en-GB"]),
        BookLanguage(book=books["003"], language=languages["en-GB"]),
        BookLanguage(book=books["004"], language=languages["en-GB"]),
        BookLanguage(book=books["005"], language=languages["en-GB"]),
        BookLanguage(book=books["006"], language=languages["en-GB"]),
        BookLanguage(book=books["007"], language=languages["en-GB"]),
        BookLanguage(book=books["008"], language=languages["en-GB"]),
        BookLanguage(book=books["001"], language=languages["fr-FR"]),
        BookLanguage(book=books["002"], language=languages["fr-FR"]),
        BookLanguage(book=books["003"], language=languages["fr-FR"]),
        BookLanguage(book=books["004"], language=languages["fr-FR"]),
        BookLanguage(book=books["005"], language=languages["fr-FR"]),
        BookLanguage(book=books["006"], language=languages["fr-FR"]),
        BookLanguage(book=books["007"], language=languages["fr-FR"]),
        BookLanguage(book=books["008"], language=languages["fr-FR"]),
    ]
    session.add_all(book_languages)
    session.flush()
    book_shelves: t.List[BookShelf] = [
        BookShelf(book=books["001"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["001"], shelf=shelves["Shelf B"]),
        BookShelf(book=books["002"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["002"], shelf=shelves["Shelf B"]),
        BookShelf(book=books["003"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["004"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["005"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["006"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["007"], shelf=shelves["Shelf A"]),
        BookShelf(book=books["008"], shelf=shelves["Shelf A"]),
    ]
    session.add_all(book_shelves)
    session.flush()
    faker = Faker()
    for _ in range(size):
        book = Book(
            isbn=faker.unique.isbn13(),
            title=faker.sentence(),
            description=faker.text(),
            publisher=random.choice(list(publishers.values())),
        )
        session.add_all(
            [
                book,
                BookAuthor(
                    book=book, author=random.choice(list(authors.values()))
                ),
                BookSubject(
                    book=book, subject=random.choice(list(subjects.values()))
                ),
                BookLanguage(
                    book=book,
                    language=random.choice(list(languages.values())),
                ),
                BookShelf(
                    book=book, shelf=random.choice(list(shelves.values()))
                ),
            ]
        )
