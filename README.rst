# PostgreSQL to Elasticsearch sync


`PGSync <https://pgsync.com>`_ is a middleware for syncing data from `Postgres <https://www.postgresql.org>`_ to `Elasticsearch <https://www.elastic.co/products/elastic-stack>`_.  
It allows you to keep `Postgres <https://www.postgresql.org>`_ as your source of truth data source and
expose structured denormalized documents in `Elasticsearch <https://www.elastic.co/products/elastic-stack>`_.


### Requirements

- `Python <https://www.python.org>`_ 3.6+
- `Postgres <https://www.postgresql.org>`_ 9.4+
- `Redis <https://redis.io>`_
- `Elasticsearch <https://www.elastic.co/products/elastic-stack>`_ 6.3.1+

### Postgres setup
  
  Enable `logical decoding <https://www.postgresql.org/docs/current/logicaldecoding.html>`_ in your 
  Postgres setting.

  - You would also need to set up two parameters in your Postgres config postgresql.conf

    ```wal_level = logical```

    ```max_replication_slots = 1```

### Installation

You can install PGSync from `PyPI <https://pypi.org>`_:

    $ pip install pgsync

### Config

Create a schema for the application named e.g **schema.json**

`Example schema <https://github.com/toluaina/pgsync/blob/master/examples/airbnb/schema.json>`_

Example spec

.. code-block::

    [
        {
            "database": "[database name]",
            "index": "[elasticsearch index]",
            "nodes": [
                {
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
            ]
        }
    ]

### Environment variables 

Setup required environment variables for the application

    SCHEMA='/path/to/schema.json'

    ELASTICSEARCH_HOST=localhost
    ELASTICSEARCH_PORT=9200

    PG_HOST=localhost
    PG_USER=i-am-root # this must be a postgres superuser
    PG_PORT=5432
    PG_PASSWORD=*****

    REDIS_HOST=redis
    REDIS_PORT=6379
    REDIS_DB=0
    REDIS_AUTH=*****


## Running

Bootstrap the database (one time only)
  $ bootstrap --config schema.json
Run pgsync as a daemon
  $ pgsync --config schema.json --daemon
