.. _changelog:

Changelog
=========

Unreleased
----------

* Preserve spaces and reserved characters in database, search, and Redis connection URL usernames and passwords.
* Find the root document of a changed child row more reliably:

  - Sync a child row that moves to a different parent. Both the old and the new root documents change.
  - Sync a child change even if its root document is not searchable yet (within the index refresh interval).
    pgsync reads the root key from the payload, or finds the root with a database query.
    The query joins up to the root only on columns with a btree or hash index.
    If a path has no such index, pgsync uses the search as before.
  - Use all the columns of a composite foreign key, not only the first one.
  - For a child delete, and for the old root of a child row that moves, pgsync needs the old foreign keys
    in the payload. Trigger mode, binlog mode with ``binlog_row_image=FULL``
    and WAL mode with ``REPLICA IDENTITY FULL`` have them.
  - Remove duplicate filters before the sync query. Many changes to one row now give one filter.

* The trigger function now sends the foreign keys of a deleted row. Only trigger mode uses it.
  To install it, do these steps:

  1. Stop the producer. Let the consumer empty the Redis queue, then stop it.
  2. Run ``bootstrap``. It replaces the trigger function, so ``bootstrap -t`` is not necessary.
  3. Start pgsync.

  ``bootstrap`` deletes the checkpoint file. If ``REDIS_CHECKPOINT`` is not set, it also clears the Redis queue,
  and the next start does a full sync. pgsync can miss a delete between steps 1 and 3.
  If you keep the old trigger function, a child delete finds its root with a search, as before.
* Count, and log at DEBUG level, the child updates and deletes that find no root document.
  If the database rejects a value in a root query (for example a WAL value of an enum key),
  log a warning one time for each table. Other database errors stop the sync as before.
* Remove ``Payload.foreign_key_constraint``. pgsync does not use it now.
* MariaDB: resume the binlog read from the checkpoint. Before, each read started at the start of the binlog.
  pgsync converts the checkpoint to a GTID position with ``BINLOG_GTID_POS``.
  If the binlog file of the checkpoint is purged, pgsync logs a warning and reads from the start, as before.
* MySQL and MariaDB: do not repeat documents and nested items. A child subquery had a second copy of its parent
  table (or through table), so the sync query gave each root row one time for each row of that table.
  This occurred for a foreign key on the parent (for example ``book.publisher_id``) and for a through table.
  The PostgreSQL query does not change.
* MySQL and MariaDB: support ``binlog_row_image=MINIMAL`` and ``NOBLOB``. Before, a column that was not
  in the row image went into the payload as a null value, and the sync stopped with an empty search query.
  Now the payload holds only the columns in the row image. An update also keeps the primary key
  from the before image. With ``MINIMAL``, a child delete or move has no old foreign keys,
  so pgsync finds the old root with a search.
* MySQL and MariaDB: fill ``_meta`` with the keys of all the tables in the document, as on PostgreSQL.
  Before, ``_meta`` held only the root key, so a search for the root of a child change found nothing.
  On MySQL and MariaDB, pgsync indexes all the documents again at each start,
  so the existing documents get the new ``_meta`` when pgsync starts.
* New status line in daemon mode, with ``key=value`` fields:
  ``Sync db=book index=book wal=0 notify=59 queue=0 consumed=59 docs=535 rate=27/s``.
  A producer only or a consumer only shows only its fields. ``rate`` is the number of items for each second.
  pgsync writes the line only when a counter changes, or after ``LOG_IDLE_INTERVAL`` seconds (default 60).
  Before, pgsync wrote a line at each ``LOG_INTERVAL`` (0.5 seconds), also when nothing changed.
  To keep the old line, set ``LOG_FORMAT=legacy``.
* A clearer schema tree at the start. A heading gives the database, the index and the schema.
  Each node shows its label, its table, the relationship type, the variant and the through tables,
  in aligned columns. The tree uses ASCII if the output encoding has no box characters.
  On a terminal, the table column is dim, unless ``NO_COLOR`` is set.
  To turn off the tree, set ``SHOW_TREE=false``.
  ``Node.display()`` has no ``prefix`` and ``leaf`` arguments now.

1.0.1 (2020-15-01)
------------------

* First release on PyPI.


1.0.1 (2020-01-01)
------------------

* RC1 release


1.1.0 (2020-04-13)
------------------

* Postgres multi schema support for multi-tennant applications
* Show resulting Query with verbose mode
* this release required you to re-bootstrap your database with 
 - bootstrap -t
 - bootstrap


1.1.1 (2020-05-18)
------------------

* Fixed authentication with Redis
* Fixed Docker build


1.1.2 (2020-06-11)
------------------

* Sync multiple indices in the same schema
* Test for replication or superuser
* Fix PG_NOTIFY error when payload exceeds 8000 bytes limit


1.1.3 (2020-06-14)
------------------

* Bug fix when syncing multiple indices in the same schema


1.1.4 (2020-06-15)
------------------

* Only create triggers for tables present in schema


1.1.5 (2020-06-16)
------------------

* Bug fix when creating multiple triggers in same schema


1.1.6 (2020-07-31)
------------------

* Bug fix when tearing down secondary schema


1.1.7 (2020-08-16)
------------------

* Fix issue #29: SQLAlchemy err: Neither 'BooleanClauseList' object nor 'Comparator' object has an attribute '_orig'


1.1.8 (2020-08-19)
------------------

* Fix issue #30: Traceback AttributeError: id


1.1.9 (2020-08-26)
------------------

* Fix issue #33: Unable to set Redis port via environment variable.


1.1.10 (2020-08-29)
------------------

* Support Amazon RDS #16
* Optimize database reflection on startup
* Show elapsed time


1.1.11 (2020-09-07)
------------------

* Support specify Elasticsearch field data type


1.1.12 (2020-09-08)
------------------

* Add support for SSL TCP/IP connection mode


1.1.13 (2020-09-09)
------------------

* Show version details with --version argument
* Fixed airbnb examples docker build


1.1.14 (2020-10-07)
------------------

* Support Elasticsearch settings for adding mapping and analyzers
