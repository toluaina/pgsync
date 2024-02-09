import click
from schema import Customer, CustomerGroup, Group
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.constants import DEFAULT_SCHEMA
from pgsync.helper import teardown
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

            customers = [
                Customer(name="CustomerA"),
                Customer(name="CustomerB"),
                Customer(name="CustomerC"),
            ]
            with subtransactions(session):
                session.add_all(customers)

            groups = [
                Group(group_name="GroupA"),
                Group(group_name="GroupB"),
                Group(group_name="GroupC"),
            ]
            with subtransactions(session):
                session.add_all(groups)

            customers_groups = [
                CustomerGroup(customer=customers[0], group=groups[0]),
                CustomerGroup(customer=customers[1], group=groups[1]),
                CustomerGroup(customer=customers[2], group=groups[2]),
            ]
            with subtransactions(session):
                session.add_all(customers_groups)

            session.commit()


if __name__ == "__main__":
    main()
