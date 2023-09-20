FROM postgres: debezium/postgres:16
COPY conf.sql /docker-entrypoint-initdb.d/
RUN chmod a+r /docker-entrypoint-initdb.d/conf.sql
