import typing as t
from random import choice

import click
import sqlalchemy as sa
from faker import Faker
from schema import Book
from sqlalchemy.orm import sessionmaker

from pgsync.base import pg_engine
from pgsync.constants import DELETE, INSERT, TG_OP, UPDATE
from pgsync.utils import config_loader, get_config, show_settings, Timer

FIELDS = {
    "isbn": "isbn13",
    "title": "sentence",
    "description": "text",
    "copyright": "word",
}


def insert_op(session: sessionmaker, model, nsize: int) -> None:
    faker: Faker = Faker()
    rows: t.Set = set()
    for _ in range(nsize):
        kwargs = {}
        for column in model.__table__.columns:
            if column.foreign_keys:
                foreign_key = list(column.foreign_keys)[0]
                pk = [
                    column.name
                    for column in foreign_key.column.table.columns
                    if column.primary_key
                ][0]
                fkey = (
                    session.query(foreign_key.column.table)
                    .order_by(sa.func.random())
                    .limit(1)
                )
                value = getattr(fkey[0], pk)
                kwargs[column.name] = value
            elif column.primary_key:
                continue
            else:
                field = FIELDS.get(column.name)
                if not field:
                    # continue
                    raise RuntimeError(f"field {column.name} not in mapping")
                value = getattr(faker, field)()
                kwargs[column.name] = value
            print(f"Inserting {model.__table__} VALUES {kwargs}")
        row = model(**kwargs)
        rows.add(row)

    with Timer(f"Created {nsize} {model.__table__} in"):
        try:
            session.add_all(rows)
            session.commit()
        except Exception as e:
            print(f"Exception {e}")
            session.rollback()


def update_op(session: sessionmaker, model, nsize: int) -> None:
    column: str = choice(list(FIELDS.keys()))
    if column not in [column.name for column in model.__table__.columns]:
        raise RuntimeError()
    faker: Faker = Faker()
    with Timer(f"Updated {nsize} {model.__table__}"):
        for _ in range(nsize):
            field = FIELDS.get(column)
            value = getattr(faker, field)()
            row = (
                session.query(model)
                .filter(getattr(model, column) != value)
                .order_by(sa.func.random())
                .limit(1)
            )
            if row:
                print(f'Updating {model.__table__} SET {column} = "{value}"')
                try:
                    setattr(row[0], column, value)
                    session.commit()
                except Exception as e:
                    print(f"Exception {e}")
                    session.rollback()


def delete_op(session: sessionmaker, model, nsize: int) -> None:
    with Timer(f"Deleted {nsize} {model.__table__}"):
        for _ in range(nsize):
            row = session.query(model).order_by(sa.func.random()).limit(1)
            pk = [
                column.name
                for column in filter(
                    lambda x: x.primary_key, model.__table__.columns
                )
            ][0]
            if row:
                try:
                    value = getattr(row[0], pk)
                    print(f"Deleting {model.__table__} WHERE {pk} = {value}")
                    session.query(model).filter(
                        getattr(model, pk) == value
                    ).delete()
                    session.commit()
                except Exception as e:
                    print(f"Exception {e}")
                    session.rollback()


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
@click.option("--daemon", "-d", is_flag=True, help="Run as a daemon")
@click.option("--nsize", "-n", default=5000, help="Number of samples")
@click.option(
    "--tg_op",
    "-t",
    help="TG_OP",
    type=click.Choice(
        TG_OP,
        case_sensitive=False,
    ),
)
def main(config, nsize, daemon, tg_op):
    show_settings()

    config: str = get_config(config)
    doc: dict = next(config_loader(config))
    database: str = doc.get("database", doc["index"])
    with pg_engine(database) as engine:
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        session = Session()
        model = Book
        func = {
            INSERT: insert_op,
            UPDATE: update_op,
            DELETE: delete_op,
        }
        # lets do only the book model for now
        while True:
            if tg_op:
                func[tg_op](session, model, nsize)
            else:
                func[choice(TG_OP)](session, model, nsize)
            if not daemon:
                break


if __name__ == "__main__":
    main()
