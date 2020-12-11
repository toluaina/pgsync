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
pytest -x -s -vv --cov=pgsync --cov-report term-missing --cov-report=xml:tests/coverage.xml tests ${@}
