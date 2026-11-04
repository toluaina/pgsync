"""A self-referencing table (adjacency list): nodes that contain nodes."""

import typing as t

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Node(Base):
    __tablename__ = "node"
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String)
    # a root node has no parent
    node_id: Mapped[t.Optional[int]] = mapped_column(
        sa.Integer, sa.ForeignKey("node.id")
    )
    children: Mapped[t.List["Node"]] = sa.orm.relationship(
        "Node", lazy="joined", join_depth=2
    )
