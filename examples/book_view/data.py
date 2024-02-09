import click
from schema import Book, Publisher
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
from pgsync.sync import Sync
from pgsync.utils import config_loader, get_config


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config):
    config: str = get_config(config)
    teardown(drop_db=False, config=config)

    for doc in config_loader(config):
        database: str = doc.get("database", doc["index"])
        with pg_engine(database) as engine:
            schema: str = doc.get("schema", DEFAULT_SCHEMA)
            connection = engine.connect().execution_options(
                schema_translate_map={None: schema}
            )
            Session = sessionmaker(bind=connection, autoflush=True)
            session = Session()

            # Bootstrap
            publishers = {
                "Oxford Press": Publisher(name="Oxford Press", is_active=True),
                "Penguin Books": Publisher(
                    name="Penguin Books", is_active=False
                ),
                "Pearson Press": Publisher(
                    name="Pearson Press", is_active=True
                ),
                "Reutgers Press": Publisher(
                    name="Reutgers Press", is_active=False
                ),
            }
            with subtransactions(session):
                session.add_all(publishers.values())

            books = {
                "001": Book(
                    isbn="001",
                    title="It",
                    description="Stephens Kings It",
                    publisher=publishers["Oxford Press"],
                ),
                "002": Book(
                    isbn="002",
                    title="The Body",
                    description="Lodsdcsdrem ipsum dodscdslor sit amet",
                    publisher=publishers["Oxford Press"],
                ),
                "003": Book(
                    isbn="003",
                    title="Harry Potter and the Sorcerer's Stone",
                    description="Harry Potter has never been",
                    publisher=publishers["Penguin Books"],
                ),
                "004": Book(
                    isbn="004",
                    title="Harry Potter and the Chamber of Secrets",
                    description="The Dursleys were so mean and hideous that summer "
                    "that all Harry Potter wanted was to get back to the "
                    "Hogwarts School for Witchcraft and Wizardry",
                    publisher=publishers["Penguin Books"],
                ),
                "005": Book(
                    isbn="005",
                    title="The 17th Suspect",
                    description="A series of shootings exposes San Francisco to a "
                    "methodical yet unpredictable killer, and a reluctant "
                    "woman decides to put her trust in Sergeant Lindsay "
                    "Boxer",
                    publisher=publishers["Pearson Press"],
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
            with subtransactions(session):
                session.add_all(books.values())

            sync: Sync = Sync(doc, validate=False)

            sync.refresh_views()


if __name__ == "__main__":
    main()
