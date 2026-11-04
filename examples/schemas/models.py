"""A parent and a child table in two Postgres schemas."""

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
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Parent.id)
    )
