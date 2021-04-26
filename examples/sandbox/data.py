import json

import click
from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import get_config
from schema import Child, Middle, Parent
from sqlalchemy.orm import sessionmaker


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(drop_db=False, config=config)
    documents = json.load(open(config))
    engine = pg_engine(
        database=documents[0].get('database', documents[0]['index'])
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    middles = [
        Middle(id=1, name="middle 1"),
        Middle(id=2, name="middle 2"),
        Middle(id=3, name="middle 3"),
        Middle(id=4, name="middle 4"),
    ]
    with subtransactions(session):
        session.add_all(middles)

    parents = [
        Parent(id=1, name="parent 1", middle_id=1),
        Parent(id=2, name="parent 2", middle_id=2),
        Parent(id=3, name="parent 3", middle_id=3),
        Parent(id=4, name="parent 4", middle_id=4),
    ]
    with subtransactions(session):
        session.add_all(parents)

    children = [
        Child(id=1, name="child 1", middle_id=1),
        Child(id=2, name="child 2", middle_id=2),
        Child(id=3, name="child 3", middle_id=3),
        Child(id=4, name="child 4", middle_id=4),
    ]
    with subtransactions(session):
        session.add_all(children)


if __name__ == '__main__':
    main()
