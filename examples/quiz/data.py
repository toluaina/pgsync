import json

import click
from schema import Answer, Category, PossibleAnswer, Question, RealAnswer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine, subtransactions
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


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
    document = json.load(open(config))
    engine = pg_engine(
        database=document[0].get(
            'database',
            document[0]['index'],
        )
    )
    Session = sessionmaker(bind=engine, autoflush=True)
    session = Session()

    # Bootstrap
    categories = {
        'Category 1': Category(
            id=1,
            uid='c001',
            text='Colours',
        ),
        'Category 2': Category(
            id=2,
            uid='c002',
            text='Weather',
        ),
    }
    with subtransactions(session):
        session.add_all(categories.values())

    questions = {
        'Question 1': Question(
            id=1,
            uid='q001',
            category_id=1,
            category_uid='c001',
            text='What is your favorite color?',
        ),
        'Question 2': Question(
            id=2,
            uid='q002',
            category_id=2,
            category_uid='c002',
            text='Is it raining outside?',
        ),
    }
    with subtransactions(session):
        session.add_all(questions.values())

    answers = {
        'Answer 1': Answer(id=1, uid='a001', text='Red'),
        'Answer 2': Answer(id=2, uid='a002', text='Yes'),
        'Answer 3': Answer(id=3, uid='a003', text='Green'),
        'Answer 4': Answer(id=4, uid='a004', text='No'),
    }
    with subtransactions(session):
        session.add_all(answers.values())

    possible_answers = {
        'Possible Answer 1': PossibleAnswer(
            question_id=1,
            question_uid='q001',
            answer_id=1,
            answer_uid='a001',
        ),
        'Possible Answer 2': PossibleAnswer(
            question_id=1,
            question_uid='q001',
            answer_id=3,
            answer_uid='a003',
        ),
        'Possible Answer 3': PossibleAnswer(
            question_id=2,
            question_uid='q002',
            answer_id=2,
            answer_uid='a002',
        ),
        'Possible Answer 4': PossibleAnswer(
            question_id=2,
            question_uid='q002',
            answer_id=4,
            answer_uid='a004',
        ),
    }
    with subtransactions(session):
        session.add_all(possible_answers.values())

    real_answers = {
        'Real Answer 1': RealAnswer(
            question_id=1,
            question_uid='q001',
            answer_id=1,
            answer_uid='a001',
        ),
        'Real Answer 4': RealAnswer(
            question_id=2,
            question_uid='q002',
            answer_id=4,
            answer_uid='a004',
        ),
    }
    with subtransactions(session):
        session.add_all(real_answers.values())


if __name__ == '__main__':
    main()
