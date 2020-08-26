# Features

Key features of PGSync include:

- Works with any PostgreSQL database (version 9.4 or later). 
- Negligible impact on database performance.
- Transactionally consistent output in Elasticsearch. This means: writes appear only when they are committed to the database, insert, update and delete operations appear in the same order as they were committed (as opposed to eventual consistency).
- Fault-tolerant: does not lose data, even if processes crash or a network interruption occurs, etc. The process can be recovered from the last checkpoint.
- Returns the data directly as Postgres JSON from the database for speed.
- Supports composite primary and foreign keys.
- Supports an arbitrary depth of nested entities i.e Tables having long chain of relationship dependencies.
- Supports Postgres JSON data fields. This means: we can extract JSON fields in a database table as a separate field in the resulting document.
- Customizable document structure.
