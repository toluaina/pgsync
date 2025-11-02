#! /bin/sh
set -u
# create database prior to running this bootstrap
source .pythonpath
source .env

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
  exit 1
fi

if [ ! -d "$(pwd)/examples/$@" ]; then
  echo "Path does not exist: $(pwd)/examples/$@"
  exit 1
fi

read -p "Are you sure you want to delete the '$@' Search index? [y/N] " -n 1 -r
echo 
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  exit 1
fi

curl -X DELETE $ELASTICSEARCH_SCHEME://$ELASTICSEARCH_HOST:$ELASTICSEARCH_PORT/$@

if [ -f "examples/$@/schema.py" ]; then
  python examples/$@/schema.py --config "$(pwd)/examples/$@/schema.json"
fi

if [ -f "examples/$@/data.py" ]; then
  python examples/$@/data.py --config "$(pwd)/examples/$@/schema.json"
fi

# Only run bootstrap for Postgres compatible backend
if [[ -z "${PG_DRIVER:-}" || "${PG_DRIVER:-}" =~ ^(psycopg|psycopg2|psycopg3|asyncpg|pg8000)$ ]]; then
  bootstrap --config "$(pwd)/examples/$1/schema.json"
fi

pgsync --config "$(pwd)/examples/$@/schema.json"
