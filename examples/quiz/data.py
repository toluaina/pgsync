import click
from schema import Answer, Category, PossibleAnswer, Question, RealAnswer
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
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
    doc = next(config_loader(config))
    database: str = doc.get("database", doc["index"])
    with pg_engine(database) as engine:
        Session = sessionmaker(bind=engine, autoflush=True)
        session = Session()

        # Bootstrap
        categories = [
            Category(
                id=1,
                uid="c001",
                text="Colours",
            ),
            Category(
                id=2,
                uid="c002",
                text="Weather",
            ),
        ]
        with subtransactions(session):
            session.add_all(categories)

        questions = [
            Question(
                id=1,
                uid="q001",
                category_id=1,
                category_uid="c001",
                text="What is your favorite color?",
            ),
            Question(
                id=2,
                uid="q002",
                category_id=2,
                category_uid="c002",
                text="Is it raining outside?",
            ),
        ]
        with subtransactions(session):
            session.add_all(questions)

        answers = [
            Answer(id=1, uid="a001", text="Red"),
            Answer(id=2, uid="a002", text="Yes"),
            Answer(id=3, uid="a003", text="Green"),
            Answer(id=4, uid="a004", text="No"),
        ]
        with subtransactions(session):
            session.add_all(answers)

        possible_answers = [
            PossibleAnswer(
                question_id=1,
                question_uid="q001",
                answer_id=1,
                answer_uid="a001",
            ),
            PossibleAnswer(
                question_id=1,
                question_uid="q001",
                answer_id=3,
                answer_uid="a003",
            ),
            PossibleAnswer(
                question_id=2,
                question_uid="q002",
                answer_id=2,
                answer_uid="a002",
            ),
            PossibleAnswer(
                question_id=2,
                question_uid="q002",
                answer_id=4,
                answer_uid="a004",
            ),
        ]
        with subtransactions(session):
            session.add_all(possible_answers)

        real_answers = [
            RealAnswer(
                question_id=1,
                question_uid="q001",
                answer_id=1,
                answer_uid="a001",
            ),
            RealAnswer(
                question_id=2,
                question_uid="q002",
                answer_id=4,
                answer_uid="a004",
            ),
        ]
        with subtransactions(session):
            session.add_all(real_answers)


if __name__ == "__main__":
    main()
