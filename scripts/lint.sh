#!/bin/sh

set -e
set -x

isort --profile black --check-only bin/* examples pgsync plugins scripts tests
black --check bin/* examples pgsync plugins scripts tests
flake8 pgsync tests examples plugins bin/*
