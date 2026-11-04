"""Books synced from materialized views instead of tables."""

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import UniqueConstraint

from pgsync.view import CreateView


class Base(DeclarativeBase):
    pass


class Publisher(Base):
    __tablename__ = "publisher"
    __table_args__ = (UniqueConstraint("name"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    name: Mapped[str] = mapped_column(sa.String, nullable=False)
    is_active: Mapped[bool] = mapped_column(sa.Boolean, default=False)


class Book(Base):
    __tablename__ = "book"
    __table_args__ = (UniqueConstraint("isbn"),)
    id: Mapped[int] = mapped_column(
        sa.Integer, primary_key=True, autoincrement=True
    )
    isbn: Mapped[str] = mapped_column(sa.String, nullable=False)
    title: Mapped[str] = mapped_column(sa.String, nullable=False)
    description: Mapped[str] = mapped_column(sa.String, nullable=True)
    copyright: Mapped[str] = mapped_column(sa.String, nullable=True)
    publisher_id: Mapped[int] = mapped_column(
        sa.Integer, sa.ForeignKey(Publisher.id, ondelete="CASCADE")
    )
    publisher: Mapped[Publisher] = sa.orm.relationship(
        Publisher,
        backref=sa.orm.backref("publishers"),
    )


def after_create(connection: sa.Connection, schema: str) -> None:
    """Create a materialized view of each table, for example book_view."""
    for table in Base.metadata.sorted_tables:
        connection.execute(
            CreateView(
                schema,
                f"{table.name}_view",
                sa.select(sa.text("*")).select_from(
                    sa.table(table.name, schema=schema)
                ),
            )
        )
