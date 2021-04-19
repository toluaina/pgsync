import json
import uuid

import click
from faker import Faker
from schema import Book
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine
from pgsync.utils import get_config, show_settings, Timer


def do_insert(session, nsize):
    faker = Faker()
    books = set([])
    for _ in range(nsize):
        book = Book(
            isbn=faker.isbn13() + str(uuid.uuid4()),
            title=faker.sentence(),
            description=faker.text(),
            publisher_id=1,
        )
        books.add(book)

    with Timer(f'Created {nsize} books in'):
        try:
            session.add_all(books)
            session.commit()
        except Exception as e:
            print(f'Exception {e}')
            session.rollback()


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
@click.option('--nsize', '-n', default=5000, help='Number of samples')
def main(config, nsize):

    show_settings()

    config = get_config(config)
    documents = json.load(open(config))
    engine = pg_engine(
        database=documents[0].get('database', documents[0]['index'])
    )
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()

    do_insert(session, nsize)


if __name__ == '__main__':
    main()
