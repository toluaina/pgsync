# Setup

  - Ensure Postgres database user is a superuser

    ```
     SELECT usename FROM pg_user WHERE usesuper = true
    ```

  - Enable logical decoding in [postgres.conf](https://www.postgresql.org/docs/current/config-setting.html)

    ```
    wal_level = logical
    ```

  - Ensure there is at least one replication slot defined in [postgres.conf](https://www.postgresql.org/docs/current/config-setting.html)

      ```
      max_replication_slots = 1
      ```
