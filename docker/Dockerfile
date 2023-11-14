FROM postgres:14.5
# FROM debezium/postgres:16
COPY conf.sql /docker-entrypoint-initdb.d/
RUN chmod a+r /docker-entrypoint-initdb.d/conf.sql
