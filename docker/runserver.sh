#! /bin/sh
./wait-for-it.sh postgres:5432 -t 60
./wait-for-it.sh elasticsearch:9200 -t 60
./wait-for-it.sh redis:6379 -t 60

python examples/airbnb/schema.py --config examples/airbnb/schema.json
python examples/airbnb/data.py --config examples/airbnb/schema.json

bootstrap --config examples/airbnb/schema.json
pgsync --config examples/airbnb/schema.json --daemon