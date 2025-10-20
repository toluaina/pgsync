#!/bin/sh

set -e

if [ ! -f .env ]; then
  echo "Env file does not exist: \".env\""
  echo "Create a \".env\" file with Postgres database settings."
  echo "See .env.sample for reference."
  exit 1
fi

source .env
source .pythonpath

# Use NullPool (no connection pooling) for tests to avoid connection exhaustion
# NullPool creates connections on demand and closes them immediately when done
# This is ideal for tests where each test creates multiple Sync() instances
export SQLALCHEMY_USE_NULLPOOL=True

pytest -x -s -vv --cov=pgsync --cov-report term-missing --cov-report=xml:tests/coverage.xml tests ${@}
