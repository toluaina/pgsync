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
