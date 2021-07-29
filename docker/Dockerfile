FROM alpine:3.13
RUN apk update && apk upgrade && apk --update add jq && rm -rf /tmp/* /var/cache/apk/*

FROM postgres: debezium/postgres:13-alpine
COPY conf.sql /docker-entrypoint-initdb.d/
RUN chmod a+r /docker-entrypoint-initdb.d/conf.sql
