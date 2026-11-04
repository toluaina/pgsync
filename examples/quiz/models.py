"""Composite primary keys and composite foreign keys."""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import ForeignKeyConstraint, UniqueConstraint


class Base(DeclarativeBase):
    pass


class Category(Base):
    __tablename__ = "category"
    __table_args__ = (UniqueConstraint("text"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    uid: Mapped[str] = mapped_column(sa.String, primary_key=True)
    text: Mapped[str] = mapped_column(sa.String, nullable=False)


class Question(Base):
    __tablename__ = "question"
    __table_args__ = (
        UniqueConstraint("text"),
        ForeignKeyConstraint(
            ["category_id", "category_uid"], ["category.id", "category.uid"]
        ),
    )
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    uid: Mapped[str] = mapped_column(sa.String, primary_key=True)
    category_id: Mapped[int] = mapped_column(sa.Integer)
    category_uid: Mapped[str] = mapped_column(sa.String)
    text: Mapped[str] = mapped_column(sa.String, nullable=False)


class Answer(Base):
    __tablename__ = "answer"
    __table_args__ = (UniqueConstraint("text"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    uid: Mapped[str] = mapped_column(sa.String, primary_key=True)
    text: Mapped[str] = mapped_column(sa.String, nullable=False)


class PossibleAnswer(Base):
    __tablename__ = "possible_answer"
    __table_args__ = (
        UniqueConstraint(
            "question_id",
            "question_uid",
            "answer_id",
            "answer_uid",
        ),
        ForeignKeyConstraint(
            ["answer_id", "answer_uid"],
            ["answer.id", "answer.uid"],
        ),
        ForeignKeyConstraint(
            ["question_id", "question_uid"],
            ["question.id", "question.uid"],
        ),
    )
    question_id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    question_uid: Mapped[str] = mapped_column(sa.String, primary_key=True)
    answer_id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    answer_uid: Mapped[str] = mapped_column(sa.String, primary_key=True)
    answer: Mapped[Answer] = sa.orm.relationship(
        Answer, backref=sa.orm.backref("answer")
    )


class RealAnswer(Base):
    __tablename__ = "real_answer"
    __table_args__ = (
        UniqueConstraint(
            "question_id",
            "question_uid",
            "answer_id",
            "answer_uid",
        ),
        ForeignKeyConstraint(
            ["answer_id", "answer_uid"],
            ["answer.id", "answer.uid"],
        ),
        ForeignKeyConstraint(
            ["question_id", "question_uid"],
            ["question.id", "question.uid"],
        ),
    )
    question_id: Mapped[int] = mapped_column(
        sa.Integer,
        primary_key=True,
    )
    question_uid: Mapped[str] = mapped_column(
        sa.String,
        primary_key=True,
    )
    answer_id: Mapped[int] = mapped_column(
        sa.Integer,
        primary_key=True,
    )
    answer_uid: Mapped[str] = mapped_column(
        sa.String,
        primary_key=True,
    )
