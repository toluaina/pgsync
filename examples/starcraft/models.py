"""Species with units and structures: two one-to-many children."""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint


class Base(DeclarativeBase):
    pass


# sourced from https://starcraft.fandom.com/wiki/List_of_StarCraft_II_units


class Specie(Base):
    __tablename__ = "specie"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class Unit(Base):
    __tablename__ = "unit"
    __table_args__ = (
        UniqueConstraint(
            "name",
        ),
    )
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    details: Mapped[str] = mapped_column(sa.String, nullable=True)
    specie_id: Mapped[int] = mapped_column(sa.Integer, nullable=False)


class Structure(Base):
    __tablename__ = "structure"
    __table_args__ = (
        UniqueConstraint(
            "name",
        ),
    )
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    details: Mapped[str] = mapped_column(sa.String, nullable=True)
    specie_id: Mapped[int] = mapped_column(sa.Integer, nullable=False)
