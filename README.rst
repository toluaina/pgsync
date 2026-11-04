======
PGSync
======

.. image:: https://img.shields.io/pypi/v/pgsync.svg
   :target: https://pypi.org/project/pgsync/
   :alt: PyPI Version

.. image:: https://img.shields.io/pypi/pyversions/pgsync.svg
   :target: https://pypi.org/project/pgsync/
   :alt: Python Versions

.. image:: https://img.shields.io/pypi/l/pgsync.svg
   :target: https://opensource.org/licenses/MIT
   :alt: License

.. image:: https://img.shields.io/pypi/dm/pgsync.svg
   :target: https://pypi.org/project/pgsync/
   :alt: Downloads

|

**PostgreSQL/MySQL/MariaDB to Elasticsearch/OpenSearch sync**

`PGSync <https://pgsync.com>`_ is a middleware for syncing data from `PostgreSQL <https://www.postgresql.org>`_, `MySQL <https://www.mysql.com>`_, or `MariaDB <https://mariadb.org>`_ to `Elasticsearch <https://www.elastic.co/products/elastic-stack>`_ or `OpenSearch <https://opensearch.org>`_.

Keep your relational database as the source of truth and expose structured denormalized documents in your search engine.

**Django integration:** Use `django-pgsync <https://github.com/toluaina/django-pgsync/>`_ to integrate PGSync with Django projects.

**PGSync Pro:** Add real-time `semantic and hybrid search <https://pgsync.com/pro>`_ to Elasticsearch or OpenSearch, with on-device or hosted embeddings that re-embed only when a document's meaning changes.

**ClickHouse connector:** Stream PostgreSQL to `ClickHouse <https://clickhouse.com>`_ in real time with `pgsync-clickhouse <https://pgsync.com/clickhouse>`_, a change-data-capture sink that mirrors tables or builds denormalized wide tables on ``ReplacingMergeTree`` (one-to-one children flattened into typed columns, one-to-many into native ``Array(Tuple(...))``), with exactly-once-on-replay keyed on the source WAL LSN.

Key Features
------------

- Real-time sync via logical decoding (PostgreSQL) or binary log (MySQL/MariaDB)
- Denormalize complex relational data into nested search documents
- JSON schema-based configuration
- Support for one-to-one, one-to-many relationships
- Plugin system for document transformation
- Multiple operation modes: daemon, polling, or direct WAL streaming
- Terminal monitor of events, throughput, slot lag, and errors (``--monitor``)

Requirements
------------

- `Python <https://www.python.org>`_ 3.10+
- `PostgreSQL <https://www.postgresql.org>`_ 9.6+ or `MySQL <https://www.mysql.com>`_ 8.0.0+ or `MariaDB <https://mariadb.org>`_ 12.0.0+
- `Redis <https://redis.io>`_ 3.1.0+ or `Valkey <https://valkey.io>`_ 7.2.0+ (optional in WAL mode)
- `Elasticsearch <https://www.elastic.co/products/elastic-stack>`_ 6.3.1+ or `OpenSearch <https://opensearch.org>`_ 1.3.7+

Installation
------------

Install from `PyPI <https://pypi.org/project/pgsync/>`_:

.. code-block:: bash

   pip install pgsync

Database Setup
--------------

PostgreSQL
~~~~~~~~~~

Enable `logical decoding <https://www.postgresql.org/docs/current/logicaldecoding.html>`_ in your PostgreSQL configuration (``postgresql.conf``):

.. code-block:: ini

   wal_level = logical
   max_replication_slots = 1

MySQL / MariaDB
~~~~~~~~~~~~~~~

Enable binary logging in your MySQL/MariaDB configuration (``my.cnf``):

.. code-block:: ini

   server-id = 1
   log_bin = mysql-bin
   binlog_row_image = FULL
   binlog_expire_logs_seconds = 604800

Create a replication user:

.. code-block:: sql

   CREATE USER 'replicator'@'%' IDENTIFIED WITH mysql_native_password BY 'password';
   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator'@'%';
   FLUSH PRIVILEGES;

Configuration
-------------

Create a JSON schema file (e.g., ``schema.json``) defining your sync mapping:

.. code-block:: json

   [
       {
           "database": "book",
           "index": "book",
           "nodes": {
               "table": "book",
               "columns": ["isbn", "title", "description"],
               "children": [
                   {
                       "table": "publisher",
                       "columns": ["name"],
                       "relationship": {
                           "variant": "object",
                           "type": "one_to_one"
                       }
                   },
                   {
                       "table": "author",
                       "label": "authors",
                       "columns": ["name", "date_of_birth"],
                       "relationship": {
                           "variant": "object",
                           "type": "one_to_many",
                           "through_tables": ["book_author"]
                       }
                   }
               ]
           }
       }
   ]

See the `examples directory <https://github.com/toluaina/pgsync/tree/main/examples>`_ for more schema examples (airbnb, social, rental, etc.).

Environment Variables
---------------------

Configure PGSync via environment variables:

.. code-block:: bash

   # Schema
   SCHEMA='/path/to/schema.json'

   # PostgreSQL
   PG_HOST=localhost
   PG_PORT=5432
   PG_USER=postgres
   PG_PASSWORD=*****

   # Elasticsearch / OpenSearch
   ELASTICSEARCH_HOST=localhost
   ELASTICSEARCH_PORT=9200

   # Redis (optional in WAL mode)
   REDIS_HOST=localhost
   REDIS_PORT=6379

Running
-------

**Bootstrap** (run once to set up triggers and replication slots):

.. code-block:: bash

   bootstrap --config schema.json

**Run as daemon**:

.. code-block:: bash

   pgsync --config schema.json --daemon

Monitor
-------

Use ``--monitor`` (or ``-m``) to see the sync events in your terminal while PGSync runs. The monitor replaces the usual status line with a full-screen panel.

.. code-block:: bash

   pip install 'pgsync[monitor]'

   # Trigger mode
   pgsync --config schema.json --daemon --monitor

   # WAL mode
   pgsync --config schema.json --wal --monitor

The header shows the health: ``healthy``, ``backlog`` (the Redis queue has 1,000 events or more), ``slot lag`` (the replication slot is 100 MB or more behind), ``checkpoint stale``, or ``errors`` (an error in the last 60 seconds).

The monitor shows these sections:

- **Pipeline:** one row for each index, with the number of events read, the Redis queue size, the replication slot lag, the documents sent, the documents in the index, and the last checkpoint. An arrow shows if the queue or the slot lag increases (``↑``) or decreases (``↓``). A column with no values does not show.
- **Progress:** a progress bar, the rate, and the estimated time to the end for the initial sync. Also a progress bar while PGSync reads the changes that occurred while it was stopped (catch-up).
- **Throughput:** events and documents each second for the last 30 seconds, the highest value, the time since the last event, and the number of errors.
- **By table:** the number of ``INSERT``, ``UPDATE``, ``DELETE``, and ``TRUNCATE`` events for the busiest tables.
- **Messages:** the last 4 different warnings and errors, with a count for a repeated message.
- **Live events:** the most recent change events, with the primary key, the transaction ID, and the changed columns. The list fills the free rows of the terminal.

Press ``q`` to stop PGSync (the same as ``Ctrl+C``). Press ``p`` to pause the event list, or to start it again. Press ``f`` to show the events of one table. Press ``d`` to see all the columns of the newest event. Press ``?`` to see the help. Press ``Esc`` to go back to the event list.

PGSync stops immediately when you press ``q`` or ``Ctrl+C``, also while a query waits for a lock. This is the same with and without ``--monitor``.

Set ``MONITOR_THEME`` to ``default`` (dark background), ``light``, ``classic`` (the terminal colors), or ``mono``. Set ``MONITOR_COLORS`` to change single colors, for example ``MONITOR_COLORS="border=magenta,insert=#00ff00"``.

``--monitor`` needs ``--daemon`` or ``--wal`` and an interactive terminal. It does not support polling mode, MySQL, or MariaDB. See the `full description <https://github.com/toluaina/pgsync#monitor>`_.

Links
-----

- **Documentation**: https://pgsync.com
- **Source Code**: https://github.com/toluaina/pgsync
- **Bug Reports**: https://github.com/toluaina/pgsync/issues
- **Sponsor**: https://github.com/sponsors/toluaina

License
-------

MIT License - see `LICENSE <https://github.com/toluaina/pgsync/blob/main/LICENSE>`_ for details.
