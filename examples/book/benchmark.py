"""Traffic generator for benchmarking pgsync against the book example.

Applies INSERT, UPDATE, DELETE or TRUNCATE batches to the book table,
committing per row so every operation becomes a distinct change event
for pgsync to sync. Reports the rate of each batch and a final summary.

Usage:
    python benchmark.py --config schema.json --tg_op INSERT --nsize 1000
    python benchmark.py --config schema.json --daemon
"""

import random
import time
import typing as t

import click
import sqlalchemy as sa
from faker import Faker
from schema import Book
from sqlalchemy.orm import Session, sessionmaker

from pgsync.base import pg_engine
from pgsync.constants import DELETE, INSERT, TG_OPS, TRUNCATE, UPDATE
from pgsync.utils import config_loader, show_settings, validate_config

FIELDS: t.Dict[str, str] = {
    "isbn": "isbn13",
    "title": "sentence",
    "description": "text",
    "copyright": "word",
    "tags": "words",
    "doc": "json",
    "publish_date": "date_time",
}

faker: Faker = Faker()


def foreign_key_pools(session: Session, model) -> t.Dict[str, t.List]:
    """Candidate values per foreign key column, fetched once per batch.

    Avoids one ORDER BY random() scan per row per column.
    """
    pools: t.Dict[str, t.List] = {}
    for column in model.__table__.columns:
        if not column.foreign_keys:
            continue
        foreign_key = next(iter(column.foreign_keys))
        pk = next(
            col.name
            for col in foreign_key.column.table.columns
            if col.primary_key
        )
        rows = session.execute(
            sa.select(sa.column(pk)).select_from(foreign_key.column.table)
        ).fetchall()
        pools[column.name] = [row[0] for row in rows]
    return pools


def fake_row(model, pools: t.Dict[str, t.List]) -> dict:
    kwargs: dict = {}
    for column in model.__table__.columns:
        if column.name in pools:
            kwargs[column.name] = random.choice(pools[column.name])
        elif column.primary_key:
            continue
        else:
            field = FIELDS.get(column.name)
            if field is None:
                raise RuntimeError(
                    f"no fake mapping for column {column.name}: "
                    f"add it to FIELDS"
                )
            kwargs[column.name] = getattr(faker, field)()
    return kwargs


def random_row(session: Session, model):
    return session.query(model).order_by(sa.func.random()).first()


def report(op: str, count: int, elapsed: float) -> None:
    rate = count / elapsed if elapsed else 0.0
    click.echo(f"{op}: {count} rows in {elapsed:.2f}s ({rate:.1f} rows/s)")


def insert_op(
    session: Session, model, nsize: int, verbose: bool = False
) -> int:
    pools = foreign_key_pools(session, model)
    started = time.monotonic()
    count = 0
    for _ in range(nsize):
        kwargs = fake_row(model, pools)
        if verbose:
            click.echo(f"INSERT {model.__table__} {kwargs}")
        try:
            session.add(model(**kwargs))
            session.commit()
            count += 1
        except sa.exc.SQLAlchemyError as exc:
            session.rollback()
            if verbose:
                click.echo(f"insert failed: {exc}")
    report("INSERT", count, time.monotonic() - started)
    return count


def update_op(
    session: Session, model, nsize: int, verbose: bool = False
) -> int:
    # pick a fakeable column that actually exists on the model
    columns = [
        column.name
        for column in model.__table__.columns
        if column.name in FIELDS
    ]
    started = time.monotonic()
    count = 0
    for _ in range(nsize):
        column = random.choice(columns)
        value = getattr(faker, FIELDS[column])()
        row = random_row(session, model)
        if row is None:
            break
        if verbose:
            click.echo(f'UPDATE {model.__table__} SET {column} = "{value}"')
        try:
            setattr(row, column, value)
            session.commit()
            count += 1
        except sa.exc.SQLAlchemyError as exc:
            session.rollback()
            if verbose:
                click.echo(f"update failed: {exc}")
    report("UPDATE", count, time.monotonic() - started)
    return count


def delete_op(
    session: Session, model, nsize: int, verbose: bool = False
) -> int:
    pk = next(
        column.name for column in model.__table__.columns if column.primary_key
    )
    started = time.monotonic()
    count = 0
    for _ in range(nsize):
        row = random_row(session, model)
        if row is None:
            break
        value = getattr(row, pk)
        if verbose:
            click.echo(f"DELETE {model.__table__} WHERE {pk} = {value}")
        try:
            session.query(model).filter(getattr(model, pk) == value).delete()
            session.commit()
            count += 1
        except sa.exc.SQLAlchemyError as exc:
            session.rollback()
            if verbose:
                click.echo(f"delete failed: {exc}")
    report("DELETE", count, time.monotonic() - started)
    return count


def truncate_op(
    session: Session, model, nsize: int, verbose: bool = False
) -> int:
    table = model.__table__.name
    if verbose:
        click.echo(f"TRUNCATE {table} CASCADE")
    started = time.monotonic()
    try:
        session.execute(sa.text(f"TRUNCATE TABLE {table} CASCADE"))
        session.commit()
    except sa.exc.SQLAlchemyError as exc:
        session.rollback()
        click.echo(f"truncate failed: {exc}")
        return 0
    report("TRUNCATE", 1, time.monotonic() - started)
    return 1


OPS: t.Dict[str, t.Callable] = {
    INSERT: insert_op,
    UPDATE: update_op,
    DELETE: delete_op,
    TRUNCATE: truncate_op,
}


@click.command()
@click.option(
    "--config",
    "-c",
    help="Schema config",
    type=click.Path(exists=True),
)
@click.option("--daemon", "-d", is_flag=True, help="Run rounds forever")
@click.option("--nsize", "-n", default=5000, help="Rows per round")
@click.option(
    "--tg_op",
    "-t",
    help="Operation to run; omit for a random one per round",
    type=click.Choice(TG_OPS, case_sensitive=False),
)
@click.option(
    "--interval",
    "-i",
    default=0.0,
    show_default=True,
    help="Seconds to pause between daemon rounds",
)
@click.option("--seed", default=None, type=int, help="RNG seed")
@click.option("--verbose", "-v", is_flag=True, help="Echo every statement")
def main(
    config: str,
    nsize: int,
    daemon: bool,
    tg_op: t.Optional[str],
    interval: float,
    seed: t.Optional[int],
    verbose: bool,
) -> None:
    """Generate change traffic against the book table."""
    show_settings(config)
    validate_config(config)

    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    doc: dict = next(config_loader(config))
    database: str = doc.get("database", doc["index"])

    totals: t.Dict[str, int] = {}
    started = time.monotonic()

    with pg_engine(database) as engine:
        Session_ = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        session = Session_()

        # only the book model for now
        model = Book
        try:
            while True:
                op = tg_op or random.choice(TG_OPS)
                count = OPS[op](session, model, nsize, verbose=verbose)
                totals[op] = totals.get(op, 0) + count
                if not daemon:
                    break
                if interval:
                    time.sleep(interval)
        except KeyboardInterrupt:
            click.echo("interrupted")
        finally:
            session.close()

    elapsed = time.monotonic() - started
    total = sum(totals.values())
    rate = total / elapsed if elapsed else 0.0
    breakdown = ", ".join(
        f"{op}={count}" for op, count in sorted(totals.items())
    )
    click.echo(
        f"summary: {total} operations in {elapsed:.1f}s ({rate:.1f}/s) "
        f"[{breakdown}]"
    )


if __name__ == "__main__":
    main()
