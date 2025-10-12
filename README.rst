# PostgreSQL to Elasticsearch/OpenSearch sync


- [PGSync](https://pgsync.com) is a middleware for syncing data from [Postgres](https://www.postgresql.org) to [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/) or [OpenSearch](https://opensearch.org/).
- It allows you to keep [Postgres](https://www.postgresql.org) as your source of truth data source and
expose structured denormalized documents in [Elasticsearch](https://www.elastic.co/products/elastic-stack)/[OpenSearch](https://opensearch.org/).


### Requirements

- [Python](https://www.python.org) 3.9+
- [Postgres](https://www.postgresql.org) 9.6+ or [MySQL](https://www.mysql.com/) 8.0.0+ or [MariaDB](https://mariadb.org/) 12.0.0+ 
- [Redis](https://redis.io) 3.1.0+ or [Valkey](https://valkey.io) 7.2.0+
- [Elasticsearch](https://www.elastic.co/products/elastic-stack) 6.3.1+ or [OpenSearch](https://opensearch.org/) 1.3.7+
- [SQLAlchemy](https://www.sqlalchemy.org) 1.3.4+

### Postgres Setup
  
  Enable [logical decoding](https://www.postgresql.org/docs/current/logicaldecoding.html) in your 
  Postgres setting.

  - You also need to set up two parameters in your Postgres config postgresql.conf

    ```wal_level = logical```

    ```max_replication_slots = 1```


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

You can install PGSync from [PyPI](https://pypi.org):

    $ pip install pgsync

### Config

Create a schema for the application named e.g **schema.json**

[Example schema](https://github.com/toluaina/pgsync/blob/main/examples/airbnb/schema.json)

Example spec

.. code-block::

    [
        {
            "database": "[database name]",
            "index": "[Elasticsearch or OpenSearch index]",
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
                        ... additional children
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
