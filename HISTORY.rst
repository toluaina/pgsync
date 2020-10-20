=======
History
=======

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
 
  * bootstrap -t
  * bootstrap


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
