# PGSync

[![PyPI version](https://badge.fury.io/py/pgsync.svg)](https://badge.fury.io/py/pgsync)
[![Build status](https://github.com/toluaina/pgsync/workflows/Build%20and%20Test/badge.svg)](https://github.com/toluaina/pgsync/actions)
[![Python versions](https://img.shields.io/pypi/pyversions/pgsync)](https://pypi.org/project/pgsync)
[![Downloads](https://img.shields.io/pypi/dm/pgsync)](https://pypi.org/project/pgsync)
[![codecov](https://codecov.io/gh/toluaina/pgsync/branch/main/graph/badge.svg?token=cvQzYkz6CV)](https://codecov.io/gh/toluaina/pgsync)
[![Sponsored by DigitalOcean](https://img.shields.io/badge/Sponsored%20by-DigitalOcean-0080FF?logo=digitalocean&logoColor=white)](https://www.digitalocean.com/?utm_medium=opensource&utm_source=pgsync)


## PostgreSQL/MySQL/MariaDB to Elasticsearch/OpenSearch sync

[PGSync](https://pgsync.com) is a middleware for syncing data from [Postgres](https://www.postgresql.org) or [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/) to [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) effortlessly.
It allows you to keep [Postgres](https://www.postgresql.org) or [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/) as your source of truth and
expose structured denormalized documents in [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/).

Changes to nested entities are propagated to [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/).
PGSync's advanced query builder then generates optimized SQL queries 
on the fly based on your schema.
PGSync's advisory model allows you to quickly move and transform large volumes of data quickly whilst maintaining relational integrity.

Simply describe your document structure or schema in JSON and [PGSync](https://pgsync.com) will 
continuously capture changes in your data and load it into [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) 
without writing any code.
[PGSync](https://pgsync.com) transforms your relational data into a structured document format.

It allows you to take advantage of the expressive power and scalability of 
[Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) directly from [Postgres](https://www.postgresql.org) or [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/). 
You don't have to write complex queries and transformation pipelines.
PGSync is lightweight, flexible and fast.

[Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) is more suited as as secondary denormalised search engine to accompany a more traditional normalized datastore.
Moreover, you shouldn't store your primary data in [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/).

So how do you then get your data into [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) in the first place? 
Tools like [Logstash](https://www.elastic.co/products/logstash) and [Kafka](https://kafka.apache.org) can aid this task but they still require a bit 
of engineering and development.

[Extract Transform Load](https://en.wikipedia.org/wiki/Extract,_transform,_load) and [Change data capture](https://en.wikipedia.org/wiki/Change_data_capture) tools can be complex and require expensive engineering effort.

Other benefits of PGSync include:
- Real-time analytics
- Reliable primary datastore/source of truth
- Scale on-demand (multiple consumers)
- Easily join multiple nested tables

## Sponsors

[PGSync](https://pgsync.com) is made possible with support from [DigitalOcean](https://www.digitalocean.com/?utm_medium=opensource&utm_source=pgsync).

<p>
  <a href="https://www.digitalocean.com/?utm_medium=opensource&utm_source=pgsync" rel="sponsored noopener noreferrer">
    <img
      src="https://opensource.nyc3.cdn.digitaloceanspaces.com/attribution/assets/SVG/DO_Logo_horizontal_blue.svg"
      alt="DigitalOcean"
      width="210"
      loading="lazy"
      decoding="async"
    >
  </a>
</p>


#### Why?

At a high level, you have data in a PostgreSQL/MySQL/MariaDB database and you want to mirror it in Elasticsearch/OpenSearch.  
This means every change to your data (***Insert***, ***Update***, ***Delete*** and ***Truncate*** statements) needs to be replicated to Elasticsearch/OpenSearch. 
At first, this seems easy and then it's not. Simply add some code to copy the data to Elasticsearch/OpenSearch after updating the database (or so called dual writes).
Writing SQL queries spanning multiple tables and involving multiple relationships are hard to write.
Detecting changes within a nested document can also be quite hard.
Of course, if your data never changed, then you could just take a snapshot in time and load it into Elasticsearch/OpenSearch as a one-off operation.

PGSync is appropriate for you if:
- [Postgres](https://www.postgresql.org) or [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/) is your read/write source of truth whilst [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) is your 
read-only search layer.
- You need to denormalize relational data into a NoSQL data source.
- Your data is constantly changing.
- You have existing data in a relational database such as [Postgres](https://www.postgresql.org) or [MySQL](https://www.mysql.com/) or [MariaDB](https://mariadb.org/) and you need a secondary NoSQL database like [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) for text-based queries or autocomplete queries to mirror the existing data without having your application perform dual writes.
- You want to keep your existing data untouched whilst taking advantage of
the search capabilities of [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) by exposing a view of your data without compromising the security of your relational data.
- Or you simply want to expose a view of your relational data for search purposes.


#### How it works

PGSync is written in Python (supporting version 3.9 onwards) and the stack is composed of: [Redis](https://redis.io)/[Valkey](https://valkey.io), [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/), [Postgres](https://www.postgresql.org)/[MySQL](https://www.mysql.com/)/[MariaDB](https://mariadb.org/), and [SQLAlchemy](https://www.sqlalchemy.org).

PGSync leverages the [logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) feature of [Postgres](https://www.postgresql.org) (introduced in PostgreSQL 9.4) to capture a continuous stream of change events.
This feature needs to be enabled in your [Postgres](https://www.postgresql.org) configuration file by setting in the postgresql.conf file:
```
wal_level = logical
```

You can select any pivot table to be the root of your document.

PGSync's query builder builds advanced queries dynamically against your schema.

PGSync operates in an event-driven model by creating triggers for tables in your database to handle notification events.

*This is the only time PGSync will ever make any changes to your database.*

**NOTE**: **If you change the structure of your PGSync's schema config, you would need to rebuild your Elasticsearch/OpenSearch indices.**
There are plans to support zero-downtime migrations to streamline this process.


#### Quickstart

There are several ways of installing and trying PGSync
 - [Running in Docker](#running-in-docker) is the easiest way to get up and running.
 - [Manual configuration](#manual-configuration) 

#### Book Demo Example (requires a DigitalOcean account)

[![Deploy to DO](https://www.deploytodo.com/do-btn-blue.svg)](https://cloud.digitalocean.com/apps/new?repo=https://github.com/toluaina/pgsync/tree/main)

Fill in the following during the setup
 - `ELASTICSEARCH_URL` e.g. https://user:pass@os-host:443
 - `REDIS_URL` e.g. rediss://default:pass@host:port/0


##### Running in Docker (Using Github Repository)

To startup all services with docker.

```
$ git checkout https://github.com/toluaina/pgsync
```

Run:
```
$ docker-compose up
```

Show the content in Elasticsearch/OpenSearch
```
$ curl -X GET http://[Elasticsearch/OpenSearch host]:9201/reservations/_search?pretty=true
```


##### Running with Docker (Using Image Repository)

To start all services with Docker, follow these steps:

1. Pull the Docker image:

  ```
  $ docker pull toluaina1/pgsync:latest
  ```

2. Run the container:

  ```
  $ docker run --rm -it \
  -e REDIS_CHECKPOINT=true \
  -e REDIS_HOST=<redis_host_address> \
  -e PG_URL=postgres://<username>:<password>@<postgres_host>/<database> \
  -e ELASTICSEARCH_URL=http://<elasticsearch_host>:9200 \
  -v "$(pwd)/schema.json:/app/schema.json" \
  toluaina1/pgsync:latest -c schema.json -d -b
  ```

Environment variable placeholders - full list [here](https://pgsync.com/env-vars):

- redis_host_address — Address of the Redis/Valkey server (e.g., host.docker.internal for local Docker setup)
- username — PostgreSQL/MySQL/MariaDB username
- password — PostgreSQL/MySQL/MariaDB password
- postgres_host — Host address for PostgreSQL/MySQL/MariaDB instance (e.g., host.docker.internal)
- database — Name of PostgreSQL/MySQL/MariaDB database
- elasticsearch_host — Address of Elasticsearch/OpenSearch instance (e.g., host.docker.internal)


##### Manual configuration

### Postgres Setup
  - Ensure the database user is a superuser 
  - Enable logical decoding. You would also need to set up at least two parameters at postgresql.conf

    ```wal_level = logical```

    ```max_replication_slots = 1```

  - To prevent your server logs from growing too large e.g when running on cloud infrastructure where there is a cost implication.
    You can optionally impose a ceiling on the replication slot size using [max_slot_wal_keep_size](https://www.postgresql.org/docs/13/runtime-config-replication.html)

    ```max_slot_wal_keep_size = 100GB```

### MySQL / MariaDB setup

- Enable binary logging in your MySQL / MariaDB setting.

- You also need to set up the following parameters in your MySQL / MariaDB config my.cnf, then restart the database server.

  ```server-id = 1``` # any non-zero unique ID

  ```log_bin = mysql-bin```

  ```binlog_row_image = FULL``` # recommended; if not supported on older MariaDB, omit

- optional housekeeping:
  ```binlog_expire_logs_seconds = 604800```   # 7 days

- You need to create a replication user with REPLICATION SLAVE and REPLICATION CLIENT privileges
    
  ```sql
  CREATE USER 'replicator'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
  GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
  FLUSH PRIVILEGES;
  ```

### Installation

  - Install PGSync from pypi using pip
    - ```$ pip install pgsync``` 
  - Create a [schema.json](https://github.com/toluaina/pgsync/blob/main/examples/airbnb/schema.json) for your document representation
  - Bootstrap the database (one time only)
    - ```bootstrap --config schema.json```
  - Run the program with 
    - ```pgsync --config schema.json```
  - Or as a daemon
    - ```pgsync --config schema.json -d```


#### Features

Key features of PGSync are:

- Easily denormalize relational data. 
- Works with any PostgreSQL database (version 9.6 or later). 
- Negligible impact on database performance.
- Transactionally consistent output in Elasticsearch/OpenSearch. This means: writes appear only when they are committed to the database, insert, update and delete operations appear in the same order as they were committed (as opposed to eventual consistency).
- Fault-tolerant: does not lose data, even if processes crash or a network interruption occurs, etc. The process can be recovered from the last checkpoint.
- Returns the data directly as Postgres/MySQL/MariaDB JSON from the database for speed.
- Supports composite primary and foreign keys.
- Supports Views and Materialized views.
- Supports an arbitrary depth of nested entities i.e Tables having long chain of relationship dependencies.
- Supports PostgreSQL/MySQL/MariaDB JSON data fields. This means: we can extract JSON fields in a database table as a separate field in the resulting document.
- Customizable document structure.


#### Requirements

- [Python](https://www.python.org) 3.9+
- [Postgres](https://www.postgresql.org) 9.6+ or [MySQL](https://www.mysql.com/) 5.7.22+ or [MariaDB](https://mariadb.org/) 10.5.0+ 
- [Redis](https://redis.io) 3.1.0+ or [Valkey](https://valkey.io) 7.2.0+
- [Elasticsearch](https://www.elastic.co/products/elastic-stack) 6.3.1+ or [OpenSearch](https://opensearch.org/) 1.3.7+
- [SQLAlchemy](https://www.sqlalchemy.org) 1.3.4+


#### Example

Consider this example of a Book library database.

**Book**

| isbn *(PK)* | title | description |
| ------------- | ------------- | ------------- |
| 9785811243570 | Charlie and the chocolate factory | Willy Wonka’s famous chocolate factory is opening at last! |
| 9788374950978 | Kafka on the Shore | Kafka on the Shore is a 2002 novel by Japanese author Haruki Murakami. |
| 9781471331435 | 1984 | 1984 was George Orwell’s chilling prophecy about the dystopian future. |

**Author**

| id *(PK)* | name |
| ------------- | ------------- |
| 1 | Roald Dahl |
| 2 | Haruki Murakami |
| 3 | Philip Gabriel |
| 4 | George Orwell |

**BookAuthor**

| id *(PK)* | book_isbn | author_id |
| -- | ------------- | ---------- |
| 1 | 9785811243570 | 1 |
| 2 | 9788374950978 | 2 |
| 3 | 9788374950978 | 3 |
| 4 | 9781471331435 | 4 |

With PGSync, we can simply define this [JSON](https://jsonapi.org) schema where the **_book_** table is the pivot.
A **_pivot_** table indicates the root of your document.

```json
{
    "table": "book",
    "columns": [
        "isbn",
        "title",
        "description"
    ],
    "children": [
        {
            "table": "author",
            "columns": [
                "name"
            ]
        }
    ]
}
```

To get this document structure in [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/)

```json
[
  {
      "isbn": "9785811243570",
      "title": "Charlie and the chocolate factory",
      "description": "Willy Wonka’s famous chocolate factory is opening at last!",
      "authors": ["Roald Dahl"]
  },
  {
      "isbn": "9788374950978",
      "title": "Kafka on the Shore",
      "description": "Kafka on the Shore is a 2002 novel by Japanese author Haruki Murakami",
      "authors": ["Haruki Murakami", "Philip Gabriel"]
  },
  {
      "isbn": "9781471331435",
      "title": "1984",
      "description": "1984 was George Orwell’s chilling prophecy about the dystopian future",
      "authors": ["George Orwell"]
  }
]
```

Behind the scenes, PGSync is generating advanced queries for you such as.

```sql
SELECT 
       JSON_BUILD_OBJECT(
          'isbn', book_1.isbn, 
          'title', book_1.title, 
          'description', book_1.description,
          'authors', anon_1.authors
       ) AS "JSON_BUILD_OBJECT_1",
       book_1.id
FROM book AS book_1
LEFT OUTER JOIN
  (SELECT 
          JSON_AGG(anon_2.anon) AS authors,
          book_author_1.book_isbn AS book_isbn
   FROM book_author AS book_author_1
   LEFT OUTER JOIN
     (SELECT 
             author_1.name AS anon,
             author_1.id AS id
      FROM author AS author_1) AS anon_2 ON anon_2.id = book_author_1.author_id
   GROUP BY book_author_1.book_isbn) AS anon_1 ON anon_1.book_isbn = book_1.isbn
```

You can also configure PGSync to rename attributes via the schema config
e.g

```json
  {
      "isbn": "9781471331435",
      "this_is_a_custom_title": "1984",
      "desc": "1984 was George Orwell’s chilling prophecy about the dystopian future",
      "contributors": ["George Orwell"]
  }
```

PGSync addresses the following challenges:
- What if we update the author's name in the database?
- What if we wanted to add another author for an existing book?
- What if we have lots of documents already with the same author we wanted to change the author name?
- What if we delete or update an author?
- What if we truncate an entire table?


#### Benefits

- PGSync is a simple to use out of the box solution for Change data capture.
- PGSync handles data deletions.
- PGSync requires little development effort. You simply define a schema config describing your data.
- PGSync generates advanced queries matching your schema directly.
- PGSync allows you to easily rebuild your indexes in case of a schema change.
- You can expose only the data you require in Elasticsearch/OpenSearch.
- Supports multiple Postgres/MySQL/MariaDB schemas for multi-tennant applications.


#### Contributing

Contributions are very welcome! Check out the [Contribution](CONTRIBUTING.rst) Guidelines for instructions.


#### License

This project is licensed under the terms of the [MIT](https://opensource.org/license/mit/) license.
Please see [LICENSE](LICENSE) for more details.

You should have received a copy of the MIT License along with **PGSync**.  
If not, see https://opensource.org/license/mit/.
