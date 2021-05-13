import json

import click
from schema import Child, GrandChild, GreatGrandChild, Parent, Surrogate
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import get_config


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(drop_db=False, config=config)
    documents = json.load(open(config))
    engine = pg_engine(
        database=documents[0].get("database", documents[0]["index"])
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    parents = [
        Parent(id=1, name="Parent A"),
        Parent(id=2, name="Parent B"),
        Parent(id=3, name="Parent C"),
    ]
    with subtransactions(session):
        session.add_all(parents)

    surrogates = [
        Surrogate(id=1, name="Surrogate A Parent A", parent_id=1),
        Surrogate(id=2, name="Surrogate B Parent A", parent_id=1),
        Surrogate(id=3, name="Surrogate A Parent B", parent_id=2),
        Surrogate(id=4, name="Surrogate B Parent B", parent_id=2),
        Surrogate(id=5, name="Surrogate A Parent C", parent_id=3),
        Surrogate(id=6, name="Surrogate B Parent C", parent_id=3),
    ]
    with subtransactions(session):
        session.add_all(surrogates)

    children = [
        Child(id=1, name="Child A Surrogate A Parent A", surrogate_id=1),
        Child(id=2, name="Child B Surrogate B Parent A", surrogate_id=2),
        Child(id=3, name="Child C Surrogate A Parent B", surrogate_id=3),
        Child(id=4, name="Child D Surrogate B Parent B", surrogate_id=4),
        Child(id=5, name="Child E Surrogate A Parent C", surrogate_id=5),
        Child(id=6, name="Child F Surrogate B Parent C", surrogate_id=6),
    ]
    with subtransactions(session):
        session.add_all(children)

    grand_children = [
        GrandChild(id=1, name="Grand Child A Child A", child_id=1),
        GrandChild(id=2, name="Grand Child B Child A", child_id=1),
        GrandChild(id=3, name="Grand Child C Child B", child_id=2),
        GrandChild(id=4, name="Grand Child D Child B", child_id=2),
        GrandChild(id=5, name="Grand Child E Child C", child_id=3),
        GrandChild(id=6, name="Grand Child F Child C", child_id=3),
        GrandChild(id=7, name="Grand Child G Child D", child_id=4),
        GrandChild(id=8, name="Grand Child H Child D", child_id=4),
        GrandChild(id=9, name="Grand Child I Child E", child_id=5),
        GrandChild(id=10, name="Grand Child J Child E", child_id=5),
        GrandChild(id=11, name="Grand Child K Child F", child_id=6),
        GrandChild(id=12, name="Grand Child L Child F", child_id=6),
    ]
    with subtransactions(session):
        session.add_all(grand_children)

    great_grand_children = [
        GreatGrandChild(
            id=1,
            name="Great Grand Child A Child A",
            grand_child_id=1,
        ),
        GreatGrandChild(
            id=2,
            name="Great Grand Child B Child B",
            grand_child_id=2,
        ),
        GreatGrandChild(
            id=3,
            name="Great Grand Child C Child C",
            grand_child_id=3,
        ),
        GreatGrandChild(
            id=4,
            name="Great Grand Child D Child D",
            grand_child_id=4,
        ),
        GreatGrandChild(
            id=5,
            name="Great Grand Child E Child E",
            grand_child_id=5,
        ),
        GreatGrandChild(
            id=6,
            name="Great Grand Child F Child F",
            grand_child_id=6,
        ),
        GreatGrandChild(
            id=7,
            name="Great Grand Child G Child G",
            grand_child_id=7,
        ),
        GreatGrandChild(
            id=8,
            name="Great Grand Child H Child H",
            grand_child_id=8,
        ),
        GreatGrandChild(
            id=9,
            name="Great Grand Child I Child I",
            grand_child_id=9,
        ),
        GreatGrandChild(
            id=10,
            name="Great Grand Child J Child J",
            grand_child_id=10,
        ),
        GreatGrandChild(
            id=11,
            name="Great Grand Child K Child K",
            grand_child_id=11,
        ),
        GreatGrandChild(
            id=12,
            name="Great Grand Child L Child L",
            grand_child_id=12,
        ),
    ]
    with subtransactions(session):
        session.add_all(great_grand_children)


if __name__ == "__main__":
    main()
