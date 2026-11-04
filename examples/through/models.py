"""A many-to-many relationship through a join table."""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customer"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)


class Group(Base):
    __tablename__ = "group"
    __table_args__ = (
        UniqueConstraint(
            "group_name",
        ),
    )
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    group_name: Mapped[str] = mapped_column(sa.String, nullable=False)


class CustomerGroup(Base):
    __tablename__ = "customer_group"
    __table_args__ = (
        UniqueConstraint(
            "customer_id",
            "group_id",
        ),
    )
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    customer_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Customer.id, ondelete="CASCADE"),
    )
    customer: Mapped[Customer] = sa.orm.relationship(
        Customer,
        backref=sa.orm.backref("customers"),
    )
    group_id: Mapped[int] = mapped_column(
        sa.Integer,
        sa.ForeignKey(Group.id, ondelete="CASCADE"),
    )
    group: Mapped[Group] = sa.orm.relationship(
        Group,
        backref=sa.orm.backref("groups"),
    )
