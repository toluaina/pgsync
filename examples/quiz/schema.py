import json

import click
import sqlalchemy as sa
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import ForeignKeyConstraint, UniqueConstraint

from pgsync.base import create_database, pg_engine
from pgsync.helper import teardown
from pgsync.utils import get_config

Base = declarative_base()


class Category(Base):
    __tablename__ = 'category'
    __table_args__ = (
        UniqueConstraint('text'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    uid = sa.Column(sa.String, primary_key=True)
    text = sa.Column(sa.String, nullable=False)


class Question(Base):
    __tablename__ = 'question'
    __table_args__ = (
        UniqueConstraint('text'),
        ForeignKeyConstraint(
            ['category_id', 'category_uid'],
            ['category.id', 'category.uid']
        ),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    uid = sa.Column(sa.String, primary_key=True)
    category_id = sa.Column(sa.Integer)
    category_uid = sa.Column(sa.String)
    text = sa.Column(sa.String, nullable=False)


class Answer(Base):
    __tablename__ = 'answer'
    __table_args__ = (
        UniqueConstraint('text'),
    )
    id = sa.Column(sa.Integer, primary_key=True)
    uid = sa.Column(sa.String, primary_key=True)
    text = sa.Column(sa.String, nullable=False)


class PossibleAnswer(Base):
    __tablename__ = 'possible_answer'
    __table_args__ = (
        UniqueConstraint(
            'question_id',
            'question_uid',
            'answer_id',
            'answer_uid',
        ),
        ForeignKeyConstraint(
            ['answer_id', 'answer_uid'],
            ['answer.id', 'answer.uid'],
        ),
        ForeignKeyConstraint(
            ['question_id', 'question_uid'],
            ['question.id', 'question.uid'],
        ),
    )
    question_id = sa.Column(
        sa.Integer,
        # sa.ForeignKey(Question.id),
        primary_key=True
    )
    question_uid = sa.Column(
        sa.String,
        # sa.ForeignKey(Question.uid),
        primary_key=True
    )
    # question = sa.orm.relationship(
    #     Question,
    #     backref=sa.orm.backref('question')
    # )
    answer_id = sa.Column(
        sa.Integer,
        primary_key=True
    )
    answer_uid = sa.Column(
        sa.String,
        primary_key=True
    )
    answer = sa.orm.relationship(
        Answer,
        backref=sa.orm.backref('answer')
    )


class RealAnswer(Base):
    __tablename__ = 'real_answer'
    __table_args__ = (
        UniqueConstraint(
            'question_id',
            'question_uid',
            'answer_id',
            'answer_uid',
        ),
        ForeignKeyConstraint(
            ['answer_id', 'answer_uid'],
            ['answer.id', 'answer.uid'],
        ),
        ForeignKeyConstraint(
            ['question_id', 'question_uid'],
            ['question.id', 'question.uid'],
        ),


    )
    question_id = sa.Column(
        sa.Integer,
        # sa.ForeignKey(Question.id),
        primary_key=True,
    )
    question_uid = sa.Column(
        sa.String,
        # sa.ForeignKey(Question.uid),
        primary_key=True,
    )
    answer_id = sa.Column(
        sa.Integer,
        primary_key=True,
    )
    answer_uid = sa.Column(
        sa.String,
        primary_key=True,
    )


def setup(config=None):
    for document in json.load(open(config)):
        database = document.get('database', document['index'])
        create_database(database)
        engine = pg_engine(database=database)
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)


@click.command()
@click.option(
    '--config',
    '-c',
    help='Schema config',
    type=click.Path(exists=True),
)
def main(config):

    config = get_config(config)
    teardown(config=config)
    setup(config)


if __name__ == '__main__':
    main()
