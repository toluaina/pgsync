"""A through table in a different Postgres schema from its parent and child."""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent"
    __table_args__ = {"schema": "parent"}
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)


class Child(Base):
    __tablename__ = "child"
    __table_args__ = {"schema": "child"}
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)


class ChildToParent(Base):
    __tablename__ = "child_to_parent"
    __table_args__ = {"schema": "child_to_parent"}
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    child_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Child.id, ondelete="CASCADE"),
    )
    child: Mapped[Child] = sa.orm.relationship(
        Child,
        backref=sa.orm.backref("children"),
    )
    parent_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Parent.id, ondelete="CASCADE"),
    )
    parent: Mapped[Parent] = sa.orm.relationship(
        Parent,
        backref=sa.orm.backref("parents"),
    )
