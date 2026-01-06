<div align="center">

# PGSync

### Real-time PostgreSQL to Elasticsearch/OpenSearch sync

**Keep your relational database as the source of truth while powering lightning-fast search**

[![PyPI version](https://img.shields.io/pypi/v/pgsync?color=blue&logo=pypi&logoColor=white)](https://pypi.org/project/pgsync)
[![Python versions](https://img.shields.io/pypi/pyversions/pgsync?logo=python&logoColor=white)](https://pypi.org/project/pgsync)
[![Build status](https://img.shields.io/github/actions/workflow/status/toluaina/pgsync/python-build.yml?branch=main&logo=github)](https://github.com/toluaina/pgsync/actions)
[![codecov](https://img.shields.io/codecov/c/github/toluaina/pgsync?logo=codecov&logoColor=white)](https://codecov.io/gh/toluaina/pgsync)
[![Downloads](https://img.shields.io/pypi/dm/pgsync?color=brightgreen&logo=pypi&logoColor=white)](https://pypi.org/project/pgsync)
[![License](https://img.shields.io/github/license/toluaina/pgsync?color=blue)](LICENSE)

[![Docker](https://img.shields.io/badge/docker-ready-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/r/toluaina1/pgsync)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

[Website](https://pgsync.com) · [Documentation](https://pgsync.com) · [Examples](examples/) · [Report Bug](https://github.com/toluaina/pgsync/issues)

</div>

---

## What is PGSync?

PGSync is a change data capture tool that syncs data from **PostgreSQL**, **MySQL**, or **MariaDB** to **Elasticsearch** or **OpenSearch** in real-time. Define your document structure in JSON, and PGSync handles the rest — no custom code required.

```mermaid
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
```

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
| ![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white) | 3.9+ |
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
