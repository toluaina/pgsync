#!/bin/sh

set -e
set -x

isort --profile black --check-only bin demo examples pgsync tests plugins
black --check bin demo examples pgsync tests plugins
