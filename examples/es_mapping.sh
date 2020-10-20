#! /bin/sh
set -u

source .pythonpath

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
  exit 1
fi

if [ ! -d "$(pwd)/examples/$@" ]; then
  echo "Path does not exist: $(pwd)/examples/$@"
  exit 1
fi

es_mapping --config "$(pwd)/examples/$@/schema.json"
