"""Sample data for the quiz example."""

import typing as t

from sqlalchemy.orm import Session

from .models import Answer, Category, PossibleAnswer, Question, RealAnswer


def seed(session: Session) -> None:
    categories: t.List[Category] = [
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
    session.add_all(categories)
    session.flush()
    questions: t.List[Question] = [
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
    session.add_all(questions)
    session.flush()
    answers: t.List[Answer] = [
        Answer(id=1, uid="a001", text="Red"),
        Answer(id=2, uid="a002", text="Yes"),
        Answer(id=3, uid="a003", text="Green"),
        Answer(id=4, uid="a004", text="No"),
    ]
    session.add_all(answers)
    session.flush()
    possible_answers: t.List[PossibleAnswer] = [
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
    session.add_all(possible_answers)
    session.flush()
    real_answers: t.List[RealAnswer] = [
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
    session.add_all(real_answers)
