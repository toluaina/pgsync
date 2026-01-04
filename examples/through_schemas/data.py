import click
from schema import Child, ChildToParent, Parent
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import config_loader, validate_config


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config: str) -> None:
    validate_config(config)
    teardown(drop_db=False, config=config)
    doc: dict = next(config_loader(config))
    database: str = doc.get("database", doc["index"])
    with pg_engine(database) as engine:
        Session = sessionmaker(bind=engine, autoflush=True)
        session = Session()
        with subtransactions(session):
            session.add_all(
                [
                    Parent(id=1, name="Parent A"),
                    Parent(id=2, name="Parent B"),
                    Parent(id=3, name="Parent C"),
                ]
            )
        with subtransactions(session):
            session.add_all(
                [
                    Child(id=1, name="Child A Parent A"),
                    Child(id=2, name="Child B Parent A"),
                    Child(id=3, name="Child C Parent B"),
                ]
            )
        with subtransactions(session):
            session.add_all(
                [
                    ChildToParent(name="Relation 1", child_id=1, parent_id=1),
                    ChildToParent(name="Relation 2", child_id=2, parent_id=1),
                    ChildToParent(name="Relation 3", child_id=3, parent_id=2),
                ]
            )
        session.commit()


if __name__ == "__main__":
    main()
