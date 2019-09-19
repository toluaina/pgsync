#! /bin/sh
set -u
# create database prior to running this bootstrap
# ensure bin dir is in the PATH
source .path
export SCHEMA='./examples/airbnb/schema.json'
read -p "Are you sure you want to delete the 'airbnb' elasticserch index? [y/N] " -n 1 -r
echo 
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi
curl -X DELETE http://localhost:9200/airbnb
python examples/airbnb/schema.py
python examples/airbnb/data.py
bootstrap
pgsync