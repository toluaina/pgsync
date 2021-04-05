FROM postgres: debezium/postgres:13-alpine
COPY conf.sql /docker-entrypoint-initdb.d/
RUN chmod a+r /docker-entrypoint-initdb.d/conf.sql
