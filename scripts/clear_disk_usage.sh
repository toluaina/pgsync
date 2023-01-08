#!/bin/sh

export $(grep -v '^#' .env | xargs)

export ELASTICSEARCH_HOST=localhost
export ELASTICSEARCH_PORT=9200

source .env

curl -XPUT -H "Content-Type: application/json" $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'
curl -XPUT -H "Content-Type: application/json" $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
curl -XPUT -H "Content-Type: application/json" $ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/_all/_settings -d '{"index.mapping.total_fields.limit": 3000}'
