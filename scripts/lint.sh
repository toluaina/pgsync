#!/bin/sh

set -e
set -x

isort pgsync tests examples demo bin --check-only
black --check pgsync tests examples demo bin 
