# Examples

Each folder is one example with a `schema.json`. Most examples also have sample data:

- `models.py`: the tables, as SQLAlchemy models.
- `data.py`: a `seed(session)` function that adds the rows.

## Run an example

Run the commands from the repository root. PGSync reads the database, search engine, and Redis settings from the environment or from `.env`, as usual.

```bash
python -m examples.run list                 # show the examples
python -m examples.run up book              # set up the example, then run pgsync
```

`up` does these steps:

1. Drop the database, the index, and the sync state (the replication slot, the Redis queue, and the checkpoint).
2. Create the database and the tables.
3. Add the sample data.
4. Run `bootstrap` (PostgreSQL only).
5. Run `pgsync --daemon`. Press `Ctrl+C` to stop it.

Put extra pgsync options after `--`:

```bash
python -m examples.run up book -- --monitor
```

Use these commands to do one step at a time:

| Command | Action |
|---------|--------|
| `setup EXAMPLE` | Drop and create the database and the tables. |
| `seed EXAMPLE` | Delete the rows, the index, the Redis queue, and the checkpoint, then add the sample data. |
| `down EXAMPLE` | Drop the database, the index, and the sync state. |

Each command uses `examples/EXAMPLE/schema.json`. To use another file, add `--config`, `--schema_url`, or `--s3_schema_url`.

> [!WARNING]
> `up`, `setup`, and `down` drop the database that the schema config names. For example, `up book` drops the database `book`. To keep a database with that name, copy `schema.json`, change `database` and `index`, and add `--config` with the copy.

Add `--size N` to `seed` or `up` for the `book` example to add N random books:

```bash
python -m examples.run up book --size 100000
```

## The examples

| Example | Shows | Sample data |
|---------|-------|-------------|
| `airbnb` | Bookings, places, and reviews: a nested chain of one-to-one children. | Yes |
| `airline` | A large real schema: the Postgres Pro demo airline database. | No, see below |
| `ancestry` | Five levels of nested children, with a surrogate table between two. | Yes |
| `book` | Books with authors and publishers: object, scalar, and through tables, transforms, and plugins. | Yes, and `--size` |
| `book_view` | Books synced from materialized views instead of tables. | Yes |
| `geo` | `point` and `polygon` columns, on a `book` table that has them. | No |
| `node` | A self-referencing table (adjacency list): nodes that contain nodes. | Yes |
| `quiz` | Composite primary keys and composite foreign keys. | Yes |
| `rental` | A large real schema: the DVD rental sample database. | No, see below |
| `schemas` | A parent and a child table in two Postgres schemas. | Yes |
| `shop` | Shop, department, and product: explicit foreign keys at every level. | Yes |
| `social` | Users, posts, comments, and tags: many-to-many through tables. | Yes |
| `starcraft` | Species with units and structures: two one-to-many children. | Yes |
| `through` | A many-to-many relationship through a join table. | Yes |
| `through_schemas` | A through table in a different Postgres schema from its parent and child. | Yes |

### Load the data for airline and rental

These examples use public sample databases. Load the database, then run `python -m examples.run up EXAMPLE`. For an example without sample data, `up` does only the `bootstrap` and `pgsync` steps.

- **airline:** download the [demo database](https://postgrespro.ru/education/demodb) (for example `demo-small.zip`), unzip it, and run the SQL file with `psql`. This creates the database `demo`.
- **rental:** download the [DVD rental database](https://www.postgresqltutorial.com/postgresql-sample-database/), then run `createdb rental` and `pg_restore -d rental dvdrental.tar`.

### Try a change

While `up` runs, change the data in another terminal and watch the index. For example, with the `through` example:

```sql
DELETE FROM customer_group;
INSERT INTO customer_group (customer_id, group_id) VALUES (1, 2), (1, 3);
```

## Other tools

- **N-gram mapping:** `python bin/es_mapping --config examples/book/schema.json` adds an n-gram analyzer to the index mapping.
- **Benchmark:** `python -m examples.book.benchmark --config examples/book/schema.json --daemon` sends a stream of changes to the `book` tables. `examples/book_view/benchmark.py` does the same for `book_view`.

## Add an example

1. Make a folder with a `schema.json`.
2. Add `models.py` with a `Base` class and the tables. Write a one-line module docstring: `list` shows it.
3. Add `data.py` with `seed(session)`. Add the rows with `session.add_all()`. Call `session.flush()` between two groups of rows if the second group refers to the first by ID. The runner commits.
4. If the example needs more objects after the tables, for example views, add `after_create(connection, schema)` to `models.py` (see `book_view`).
