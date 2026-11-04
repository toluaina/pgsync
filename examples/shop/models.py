"""Shop, department, and product: explicit foreign keys at every level.

A regression check: the middle node's foreign key to its parent must not
leak into the join with its child (product.department_id = department.id).
"""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Shop(Base):
    __tablename__ = "shop"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    city: Mapped[str] = mapped_column(sa.String, nullable=True)


class Department(Base):
    __tablename__ = "department"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    shop_id: Mapped[int] = mapped_column(sa.Integer, sa.ForeignKey(Shop.id))
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    floor: Mapped[int] = mapped_column(sa.Integer, nullable=True)


class Product(Base):
    __tablename__ = "product"
    __table_args__ = ()
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    department_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Department.id)
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    price: Mapped[float] = mapped_column(sa.Numeric, nullable=True)
