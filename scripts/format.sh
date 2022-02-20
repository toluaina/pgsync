#!/bin/sh -e

set -x

isort --profile black bin/* demo examples pgsync plugins scripts tests setup.py
black bin/* demo examples pgsync plugins scripts tests setup.py
