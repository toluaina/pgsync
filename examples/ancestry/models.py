"""Five levels of nested children, with a surrogate table between two."""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)


class Surrogate(Base):
    __tablename__ = "surrogate"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Parent.id)
    )


class Child(Base):
    __tablename__ = "child"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Surrogate.id)
    )


class GrandChild(Base):
    __tablename__ = "grand_child"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Child.id))


class GreatGrandChild(Base):
    __tablename__ = "great_grand_child"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    parent_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(GrandChild.id)
    )
