#!/bin/sh

set -e
set -x

isort --check-only bin demo examples pgsync tests
black --check bin demo examples pgsync tests
