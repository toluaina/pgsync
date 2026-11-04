<div align="center">

# PGSync

### Real-time PostgreSQL to Elasticsearch/OpenSearch sync

**Keep your relational database as the source of truth while powering lightning-fast search**

[![PyPI version](https://img.shields.io/pypi/v/pgsync?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/pgsync)
[![Python versions](https://img.shields.io/pypi/pyversions/pgsync?logo=python&logoColor=white)](https://pypi.org/project/pgsync)
[![CI status](https://img.shields.io/github/actions/workflow/status/toluaina/pgsync/ci.yml?branch=main&logo=github)](https://github.com/toluaina/pgsync/actions)
[![codecov](https://img.shields.io/codecov/c/github/toluaina/pgsync?logo=codecov&logoColor=white)](https://codecov.io/gh/toluaina/pgsync)
[![Downloads](https://img.shields.io/pypi/dm/pgsync?color=brightgreen&logo=pypi&logoColor=white)](https://pypi.org/project/pgsync)
[![License](https://img.shields.io/github/license/toluaina/pgsync?color=blue)](LICENSE)

[![Docker](https://img.shields.io/badge/docker-ready-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/r/toluaina1/pgsync)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[Live Demo ↗](https://demo.pgsync.com/) · [Website](https://pgsync.com) · [Documentation](https://pgsync.com) · [Examples](examples/) · [Report Bug](https://github.com/toluaina/pgsync/issues)

</div>

---

## What is PGSync?

PGSync is a change data capture tool that syncs data from **PostgreSQL**, **MySQL**, or **MariaDB** to **Elasticsearch** or **OpenSearch** in real-time. Define your document structure in JSON, and PGSync handles the rest — no custom code required.

> **Django integration:** Use [django-pgsync](https://github.com/toluaina/django-pgsync/) to integrate PGSync with Django projects.

> **PGSync Pro:** Add real-time [semantic and hybrid search](https://pgsync.com/pro) to Elasticsearch or OpenSearch, with on-device or hosted embeddings that re-embed only when a document's meaning changes.

> **ClickHouse connector:** Stream PostgreSQL to [ClickHouse](https://clickhouse.com) in real time with [pgsync-clickhouse](https://pgsync.com/clickhouse), a change-data-capture sink that mirrors tables or builds denormalized wide tables on `ReplacingMergeTree` (one-to-one children flattened into typed columns, one-to-many into native `Array(Tuple(...))`), with exactly-once-on-replay keyed on the source WAL LSN.

```mermaid
%%{init: {'look': 'handDrawn', 'theme': 'neutral'}}%%
flowchart LR
    subgraph Source["🗄️ Source Database"]
        DB[(PostgreSQL<br/>MySQL<br/>MariaDB)]
    end

    subgraph CDC["⚡ Change Data Capture"]
        P[PGSync]
    end

    subgraph Search["🔍 Search Engine"]
        ES[(Elasticsearch<br/>OpenSearch)]
    end

    DB -->|WAL / Binlog| P
    P -->|Bulk Index| ES
```

### Key Features

| Feature | Description |
|---------|-------------|
| **Real-time sync** | Changes propagate instantly via logical replication |
| **Zero code** | Define mappings in JSON — no ETL pipelines to build |
| **Nested documents** | Automatically denormalize complex relationships |
| **Fault tolerant** | Resumes from checkpoints after crashes |
| **Transactionally consistent** | Documents appear in commit order |
| **Minimal overhead** | Lightweight CDC with negligible database impact |
| **Terminal monitor** | Watch events, throughput, slot lag, and errors as they happen with `--monitor` |

---

## Quick Start

### Using Docker (Fastest)

```bash
docker run --rm -it \
  -e PG_URL=postgres://user:pass@host/db \
  -e ELASTICSEARCH_URL=http://localhost:9200 \
  -e REDIS_HOST=localhost \
  -v "$(pwd)/schema.json:/app/schema.json" \
  toluaina1/pgsync:latest -c schema.json -d -b
```

### Using pip

```bash
pip install pgsync
```

```bash
# Bootstrap (one-time setup)
bootstrap --config schema.json

# Run sync
pgsync --config schema.json -d

# Run sync with a terminal monitor of events
pip install 'pgsync[monitor]'
pgsync --config schema.json -d --monitor
```

See [Monitor](#monitor) for the full description.

### Using Docker Compose

**Default (Elasticsearch + Kibana):**
```bash
git clone https://github.com/toluaina/pgsync
cd pgsync
docker-compose up
```

This starts PostgreSQL, Redis, Elasticsearch, Kibana, and PGSync configured for Elasticsearch.

**For OpenSearch:**
```bash
docker-compose --profile opensearch up
```

This starts PostgreSQL, Redis, OpenSearch, and PGSync configured for OpenSearch.

**Ports:**
- PostgreSQL: `15432`
- Elasticsearch: `9201` (default)
- Kibana: `5601` (default)
- OpenSearch: `9400` (OpenSearch profile)

---

## How It Works

**1. Define your schema** — Map tables to document structure:

```json
{
  "table": "book",
  "columns": ["isbn", "title", "description"],
  "children": [{
    "table": "author",
    "columns": ["name"]
  }]
}
```

**2. PGSync generates optimized queries** — Complex JOINs handled automatically:

```sql
SELECT JSON_BUILD_OBJECT(
  'isbn', book.isbn,
  'title', book.title,
  'authors', (SELECT JSON_AGG(author.name) FROM author ...)
) FROM book
```

**3. Get denormalized documents** — Ready for search:

```json
{
  "isbn": "9785811243570",
  "title": "Charlie and the Chocolate Factory",
  "authors": ["Roald Dahl"]
}
```

Changes to any related table automatically update the document in Elasticsearch/OpenSearch.

---

## Requirements

| Component | Version |
|-----------|---------|
| ![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white) | 3.10+ |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-9.6+-4169E1?logo=postgresql&logoColor=white) | 9.6+ (or MySQL 5.7.22+ / MariaDB 10.5+) |
| ![Elasticsearch](https://img.shields.io/badge/Elasticsearch-6.3+-005571?logo=elasticsearch&logoColor=white) | 6.3.1+ (or OpenSearch 1.3.7+) |
| ![Redis](https://img.shields.io/badge/Redis-3.1+-DC382D?logo=redis&logoColor=white) | 3.1+ (or Valkey 7.2+) — optional in WAL mode |

---

## Database Setup

<details>
<summary><b>PostgreSQL</b></summary>

Enable logical decoding in `postgresql.conf`:

```ini
wal_level = logical
max_replication_slots = 1
```

Optionally limit WAL size:

```ini
max_slot_wal_keep_size = 100GB
```

</details>

<details>
<summary><b>MySQL / MariaDB</b></summary>

Enable binary logging in `my.cnf`:

```ini
server-id = 1
log_bin = mysql-bin
binlog_row_image = FULL
binlog_expire_logs_seconds = 604800
```

Create replication user:

```sql
CREATE USER 'replicator'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
FLUSH PRIVILEGES;
```

</details>

---

## Example

Consider a book library with related authors:

| **Book** | | | |
|----------|--|--|--|
| isbn *(PK)* | title | description |
| 9785811243570 | Charlie and the Chocolate Factory | Willy Wonka's famous... |
| 9781471331435 | 1984 | George Orwell's chilling... |

| **Author** | |
|------------|--|
| id *(PK)* | name |
| 1 | Roald Dahl |
| 4 | George Orwell |

PGSync transforms this into search-ready documents:

```json
[
  {
    "isbn": "9785811243570",
    "title": "Charlie and the Chocolate Factory",
    "authors": ["Roald Dahl"]
  },
  {
    "isbn": "9781471331435",
    "title": "1984",
    "authors": ["George Orwell"]
  }
]
```

**Any change** — updating an author's name, adding a new book, deleting a relationship — is automatically synced.

---

## Transforms

PGSync supports built-in transforms to modify field values before indexing. Transforms are applied in order: `replace` → `rename` → `concat`.

### Replace

Find and replace substrings within field values:

```json
{
  "table": "product",
  "columns": ["code", "name"],
  "transform": {
    "replace": {
      "code": {
        "-": "/",
        "_": " "
      }
    }
  }
}
```

| Before | After |
|--------|-------|
| `ABC-DEF_GHI` | `ABC/DEF GHI` |

### Rename

Rename fields in the output document:

```json
{
  "table": "book",
  "columns": ["id", "title"],
  "transform": {
    "rename": {
      "id": "book_id",
      "title": "book_title"
    }
  }
}
```

### Concat

Combine multiple fields into a new field:

```json
{
  "table": "user",
  "columns": ["first_name", "last_name"],
  "transform": {
    "concat": {
      "columns": ["first_name", "last_name"],
      "destination": "full_name",
      "delimiter": " "
    }
  }
}
```

### Combined Example

Transforms can be combined and applied to nested children:

```json
{
  "table": "book",
  "columns": ["isbn", "title"],
  "children": [{
    "table": "publisher",
    "columns": ["code", "name"],
    "transform": {
      "replace": { "code": { "-": "." } },
      "rename": { "name": "publisher_name" }
    }
  }],
  "transform": {
    "concat": {
      "columns": ["isbn", "title"],
      "destination": "search_text",
      "delimiter": " - "
    }
  }
}
```

---

## Monitor

Use `--monitor` (or `-m`) to see the sync events in your terminal while PGSync runs. The monitor replaces the usual status line with a full-screen panel. The panel redraws up to 4 times each second. If no value changes, it redraws one time each second. Press `?` in the monitor to see the help.

**Install** the optional `rich` dependency:

```bash
pip install 'pgsync[monitor]'
```

**Run** PGSync with `--monitor`:

```bash
# Trigger mode
pgsync --config schema.json --daemon --monitor

# WAL mode
pgsync --config schema.json --wal --monitor
```

```
╭─ pgsync 8.0.0 ── ● healthy ── up 12m34s ── 14:32:08 ─────────────────────────────────────────────╮
│ PIPELINE  1 index                                                                                │
│ index  mode     xlog         db  redis  slot lag    sent  opensearch  checkpoint  age            │
│ ─────────────────────────────────────────────────────────────────────────────────────            │
│ book   trigger     0  1,204,883  312 ↑  1.2 MB ↓  98,412   1,204,611      884212   2s            │
│                                                                                                  │
│ THROUGHPUT  last 30s                                                                             │
│ events  ▅▇▄▆▇█▃▃▇▅▇▅▄▇▇▇▇▆▄▅▄▇▆▃▃▄▃▅▃▇  842/s  peak 894  last event  0s ago                      │
│ docs    ▇▇▄▅▆▆▇▇▅▆▇▇▅▅▆▇▆▅█▇▃▇▄▃▃▃▅▄▄▇  611/s  peak 693  errors      0                           │
│                                                                                                  │
│ BY TABLE  4 tables                                                                               │
│ table               INSERT  UPDATE  DELETE  TRUNCATE                                             │
│ ────────────────────────────────────────────────────                                             │
│ public.book          4,102  12,880     211         0                                             │
│ public.author          380   1,044       9         0                                             │
│ public.book_author     912       0     140         0                                             │
│ public.publisher        12      88       0         0                                             │
│                                                                                                  │
│ MESSAGES                                                                                         │
│ 14:31:27  WARNING  opensearch: POST http://localhost:9200/_bulk [status:429]  ×14                │
│                                                                                                  │
│ LIVE EVENTS                                                                                      │
│ 14:32:07.912  UPDATE    public.book         id=48121                    xmin=884212  title, pri… │
│ 14:32:07.905  INSERT    public.book_author  book_id=3150 author_id=679  xmin=884212              │
│ 14:32:07.871  DELETE    public.book         id=11007                    xmin=884210              │
│ 14:32:07.840  UPDATE    public.author       id=553                      xmin=884209  name        │
│ 14:32:07.802  INSERT    public.book         id=48990                    xmin=884208              │
│ 14:32:07.744  INSERT    public.audit_log    id=7781                     xmin=884206  skipped     │
│ 14:32:07.700  UPDATE    public.publisher    id=4                        xmin=884205  name        │
│ 14:32:07.640  INSERT    public.book         id=48987                    xmin=884204              │
╰─ q quit · p pause · f filter · d details · ? help ───────────────────────────────────────────────╯
```

In a color terminal, each operation has a color: INSERT is green, UPDATE is yellow, DELETE is red, and TRUNCATE is magenta.

### Header

The header shows the PGSync version, the health, the time since start (`up`), and the clock. The health is one of these values:

| Health | Color | Condition |
|--------|-------|-----------|
| `errors` | Red | PGSync logged an error in the last 60 seconds. |
| `checkpoint stale` | Yellow | Events arrive, but the checkpoint has not changed for more than 60 seconds. |
| `slot lag` | Yellow | The replication slot is 100 MB or more behind. |
| `backlog` | Yellow | The Redis queue has 1,000 events or more. |
| `healthy` | Green | None of the conditions above is true. |

If more than one condition is true, the header shows the first value in the table.

### Sections

| Section | Content |
|---------|---------|
| **Pipeline** | One row for each index in the schema config. The columns are in the table below. |
| **Progress** | The initial sync and the catch-up. This section shows only while one of them runs. See [Progress](#progress). |
| **Throughput** | Events each second and documents each second for the last 30 seconds, with the highest value (`peak`). Also the time since the last event, and the number of errors. In a narrow terminal, the history is shorter, and the heading shows the number of seconds. |
| **By table** | The number of `INSERT`, `UPDATE`, `DELETE`, and `TRUNCATE` events for each table. The busiest table is at the top. If there are too many tables, the last row shows the number of the other tables, for example `… 31 more tables`. |
| **Messages** | The last 4 different warnings and errors. If a message repeats, it moves to the top and shows a count, for example `×14`. This section shows only when there is a message. |
| **Live events** | The most recent change events. The newest event is at the top. The list fills the free rows of the terminal. Press `f` to show the events of one table. The heading shows the selected table and `paused`. |

The **Pipeline** columns are below. If a column has no value for all the indices, the monitor does not show the column. For example, in WAL mode without Redis, the `db` and `redis` columns do not show.

| Column | Meaning |
|--------|---------|
| `index` | The index. If the database has a different name, the column shows `database:index`. |
| `mode` | `WAL`, `trigger`, `trigger producer`, or `trigger consumer`. |
| `xlog` | The number of change events that PGSync read from the replication slot. |
| `db` | The number of trigger notifications that PGSync received from the database. This column shows only in trigger mode. |
| `redis` | The number of events in the Redis queue. PGSync reads this value one time each second. The value is yellow at 1,000 events or more, and red at 10,000 events or more. |
| `slot lag` | The bytes of WAL that the replication slot has not confirmed. PGSync reads this value one time each 5 seconds. The value is yellow at 100 MB or more, and red at 1 GB or more. The value is `-` if PGSync cannot read it (for example, on a standby). |
| `sent` | The number of documents that PGSync sent to the search engine since start. |
| `opensearch` or `elasticsearch` | The number of documents in the index. PGSync reads this value one time each 10 seconds. The column name is the name of the engine. If the indices use different engines, the column name is `indexed`. |
| `checkpoint` | The last checkpoint (a transaction ID). The value is `-` if there is no checkpoint. |
| `age` | The time since the checkpoint last changed. The value is red if events arrive, but the checkpoint has not changed for more than 60 seconds. |

An arrow follows the `redis` and `slot lag` values:

| Arrow | Meaning |
|-------|---------|
| `↑` (yellow) | The value increases. For `redis`, the monitor compares the value with the value 5 seconds before. For `slot lag`, the monitor compares the last 2 values. |
| `↓` (green) | The value decreases. |
| No arrow | The value does not change, or there are not sufficient values. |

Each row in **Live events** shows these items:

- The time of the event, in milliseconds.
- The operation and the table, as `schema.table`.
- The primary key values. PGSync gets the primary key columns from the schema tree. If PGSync cannot find them, it shows the first column of the row.
- The transaction ID (`xmin`). Events that PGSync reads from the replication slot at startup, or in WAL mode, do not have this value.
- The changed columns. This item shows only for an `UPDATE` event that has the old row values. In WAL mode, the old row has only the replica identity columns. Thus, the changed columns show only if the table has `REPLICA IDENTITY FULL`.
- The word `skipped`, in dim text, if the table is not in the schema (for example, a table that has a trigger but no node). PGSync does not sync these events.

The **errors** count includes each log record at level `ERROR` or `CRITICAL`.

### Progress

The **Progress** section shows two types of rows:

```
PROGRESS
initial sync  book  ━━━━━━━━━━━━━━━━━━━━━━━━  27.1%  81,428 / ~300,011  11,250/s  eta 19s
catch-up      book  ━━━━━━━━━━━━━━━━━━━━━━━━  24.0%  1,200 / 5,000
```

- **initial sync:** PGSync reads all the rows of the root table, because there is no checkpoint. The row shows the documents sent, the estimated total, the rate, and the estimated time to the end (`eta`). The total comes from the Postgres planner estimate (`pg_class.reltuples`), so it is approximate (`~`). The percentage stays below 100% until the scan ends. If there is no estimate (for example, before the first `ANALYZE`), the row shows only the count and the rate. When the scan ends, the row shows `done` and the total time for 10 seconds.
- **catch-up:** PGSync reads the changes that occurred while it was stopped. The row disappears 5 seconds after the last update.

### Event Details

Press `d` to see all the columns of the newest event. If you selected a table with `f`, the view shows the newest event of that table. The view is a snapshot: new events do not change it. Press `d` again for a new snapshot of the newest event.

```
EVENT DETAILS  d or esc to close
14:32:07.912  UPDATE  public.book  id=48121  xmin=884212
column  old       new
───────────────────────
id      48121     48121
title   'Dune'    'Dune (2nd ed)'
price   9.99      12.5
```

- For an `UPDATE`, the view shows the old and the new values. A changed column has a yellow name, a red old value, and a green new value.
- For an `INSERT`, the view shows the new values. For a `DELETE`, the view shows the old values.
- If the terminal is too short, the last row shows the number of the other columns, for example `… 12 more columns`.
- In trigger mode, the trigger sends only the primary key and foreign key columns. Thus, the view shows only these columns, and a note shows below the table. To see all the columns, use WAL mode.

### Keys

| Key | Action |
|-----|--------|
| `q` | Stop PGSync. This key does the same thing as `Ctrl+C`. |
| `p` | Pause the event list, or start it again. The counters continue while the list is paused. |
| `f` | Show the events of one table. Each press selects the next table, from the busiest table to the quietest. After the last table, the list shows all tables again. |
| `d` | Show the details of the newest event, or close the details. See [Event Details](#event-details). |
| `?` | Show the help, or close it. The help shows the keys, the health values, the column meanings, and the operation colors. |
| `Esc` | Close the details or the help, and show the event list. |

When the details or the help show, the **By table** and **Live events** sections do not show.

### Stop

Press `q` or `Ctrl+C` to stop PGSync. PGSync stops immediately, also while it waits for a slow query or for a lock in the database:

- PGSync cancels the running query on the server. Then it closes the monitor and prints `Aborted!`.
- In `--daemon` mode, PGSync does not wait for its worker threads.
- In `--wal` mode with more than one entry in the schema config, a running query can delay the stop until the query ends.

This behavior is the same with and without `--monitor`.

### Colors

Set `MONITOR_THEME` to select a color theme:

| Theme | Use |
|-------|-----|
| `default` | Bright colors for a dark terminal background. |
| `light` | Darker colors for a light terminal background. |
| `classic` | The 16 terminal colors. Your terminal theme decides how they look. |
| `mono` | No color. The monitor uses only bold and dim text. |

The `default` and `light` themes use exact colors (hex values), so they look the same in all terminals. If your terminal cannot show these colors, the monitor uses the nearest color that the terminal can show.

Set `MONITOR_COLORS` to change single colors. Write a comma-separated list of `role=style` items:

```bash
MONITOR_THEME=light MONITOR_COLORS="border=magenta,insert=bold #00ff00" \
  pgsync --config schema.json --daemon --monitor
```

A style is a [rich style](https://rich.readthedocs.io/en/stable/style.html), for example `red`, `bright_cyan`, `#ff8800`, or `bold #ff8800`. An empty style removes the color. These are the roles:

| Role | Where the monitor uses it |
|------|---------------------------|
| `border` | The panel border. |
| `accent` | The Redis queue, the events sparkline, and the progress bars. |
| `docs` | The documents sparkline and the `sent` column. |
| `ok` | `healthy`, a decreasing value (`↓`), `done`, and a new value in the details. |
| `warn` | A warning, an increasing value (`↑`), and a changed column in the details. |
| `alert` | An error, and an old value in the details. |
| `track` | The empty part of a progress bar. |
| `insert`, `update`, `delete`, `truncate` | The operation names. |

If the theme, a role, or a style is not valid, PGSync stops with a usage error. If the `NO_COLOR` environment variable is set, the monitor shows no color.

### Supported Modes

| Mode | Supported | Note |
|------|-----------|------|
| `--daemon` (trigger mode) | Yes | |
| `--daemon --consumer` | Yes | |
| `--daemon --producer` | Yes | The event list stays empty. The consumer process shows the events. |
| `--wal` | Yes | |
| `--polling` | No | |
| MySQL / MariaDB | No | PGSync always uses polling mode for MySQL and MariaDB. |
| `--analyze` | No | |

If you use `--monitor` in a mode that is not supported, PGSync stops with a usage error. PGSync also stops with a usage error in these conditions:

- `rich` is not installed.
- The output is not an interactive terminal.

### Notes

- **Logs:** while the monitor is on, PGSync removes the console log handlers. Log records at `WARNING` and above go to the **Messages** section. Log handlers that write to a file do not change. When the monitor stops, PGSync puts the console log handlers back.
- **Console output:** while the monitor is on, PGSync discards the text that it writes to stdout, for example the schema tree, the status line, and the elapsed time. The monitor shows the same information. Text written to stderr goes to the **Messages** section.
- **Errors:** if PGSync stops because of an error, the monitor closes first. Then the error prints in the terminal.
- **Many schemas:** if the schema config has more than one entry, the **Pipeline** section has one row for each entry. The other sections show the events of all entries together.
- **Separate processes:** if you run the producer and the consumer as separate processes, each process shows its own monitor.
- **Terminal size:** the event list uses all the free rows of the terminal. Make the terminal taller to see more events. The monitor keeps the last 200 events. If the terminal is very short, the event list shows 1 event.
- **Narrow terminal:** if a row of the event list is too long, the monitor cuts the changed columns first, and then the table name and the primary key. A cut value ends with `…`. The time, the operation, and `xmin` always show in full. The throughput history and the progress bars also become shorter. The monitor works best with 80 columns or more.
- **Network reads:** a separate thread reads the Redis queue, the slot lag, and the index count. A slow server does not stop the screen.
- **Slot lag:** the slot lag compares the slot to the current WAL position of the whole Postgres server. Writes to other databases on the same server also add to the lag, until PGSync confirms its next transaction.
- **Do not use `--monitor` in Docker, systemd, or with output to a file.** The monitor needs an interactive terminal. Use the usual status line in these environments.

---

## Why PGSync?

| Challenge | PGSync Solution |
|-----------|-----------------|
| Dual writes are error-prone | Captures changes from WAL — single source of truth |
| Complex JOIN queries | Auto-generates optimized SQL from your schema |
| Nested document updates | Detects changes in any related table |
| Data consistency | Transactionally consistent, ordered delivery |
| Crash recovery | Checkpoint-based resumption |

---

## Environment Variables

Full list at [pgsync.com/env-vars](https://pgsync.com/env-vars)

| Variable | Description |
|----------|-------------|
| `PG_URL` | PostgreSQL connection string |
| `ELASTICSEARCH_URL` | Elasticsearch/OpenSearch URL |
| `REDIS_HOST` | Redis/Valkey host |
| `REDIS_CHECKPOINT` | Use Redis for checkpoints (recommended for production) |

---

## One-Click Deploy

[![Deploy to DigitalOcean](https://www.deploytodo.com/do-btn-blue.svg)](https://cloud.digitalocean.com/apps/new?repo=https://github.com/toluaina/pgsync/tree/main)

---

## Sponsors

<a href="https://www.digitalocean.com/?utm_medium=opensource&utm_source=pgsync">
  <img src="https://opensource.nyc3.cdn.digitaloceanspaces.com/attribution/assets/SVG/DO_Logo_horizontal_blue.svg" alt="DigitalOcean" width="200">
</a>

---

## Contributing

Contributions welcome! See [CONTRIBUTING.rst](CONTRIBUTING.rst) for guidelines.

## License

[MIT](LICENSE) — use it freely in your projects.
