#!/bin/sh -e

set -x

isort --profile black bin/* examples pgsync plugins scripts tests setup.py
black bin/* examples pgsync plugins scripts tests setup.py
