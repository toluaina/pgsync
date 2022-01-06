# PostgreSQL to Elasticsearch sync


- [PGSync](https://pgsync.com) is a middleware for syncing data from [Postgres](https://www.postgresql.org) to [Elasticsearch](https://www.elastic.co/products/elastic-stack).
- It allows you to keep [Postgres](https://www.postgresql.org) as your source of truth data source and
expose structured denormalized documents in [Elasticsearch](https://www.elastic.co/products/elastic-stack).


### Requirements

- [Python](https://www.python.org) 3.7+
- [Postgres](https://www.postgresql.org) 9.6+
- [Redis](https://redis.io) 3.1.0
- [Elasticsearch](https://www.elastic.co/products/elastic-stack) 6.3.1+
- [SQlAlchemy](https://www.sqlalchemy.org) 1.3.4+

### Postgres setup
  
  Enable [logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) in your 
  Postgres setting.

  - You also need to set up two parameters in your Postgres config postgresql.conf

    ```wal_level = logical```

    ```max_replication_slots = 1```

### Installation

You can install PGSync from [PyPI](https://pypi.org):

    $ pip install pgsync

### Config

Create a schema for the application named e.g **schema.json**

[Example schema](https://github.com/toluaina/pgsync/blob/master/examples/airbnb/schema.json)

Example spec

.. code-block::

    [
        {
            "database": "[database name]",
            "index": "[elasticsearch index]",
            "nodes": {
                "table": "[table A]",
                "schema": "[table A schema]",
                "columns": [
                    "column 1 from table A",
                    "column 2 from table A",
                    ... additional columns
                ],
                "children": [
                    {
                        "table": "[table B with relationship to table A]",
                        "schema": "[table B schema]",
                        "columns": [
                          "column 1 from table B",
                          "column 2 from table B",
                          ... additional columns
                        ],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_many"
                        },
                        ...
                    },
                    {
                        ... any other additional children
                    }
                ]
            }
        }
    ]

### Environment variables 

Setup environment variables required for the application

    SCHEMA='/path/to/schema.json'

    ELASTICSEARCH_HOST=localhost
    ELASTICSEARCH_PORT=9200

    PG_HOST=localhost
    PG_USER=i-am-root # this must be a postgres superuser or replication user
    PG_PORT=5432
    PG_PASSWORD=*****

    REDIS_HOST=redis
    REDIS_PORT=6379
    REDIS_DB=0
    REDIS_AUTH=*****


### Running

Bootstrap the database (one time only)
  - $ bootstrap --config schema.json

Run pgsync as a daemon
  - $ pgsync --config schema.json --daemon
