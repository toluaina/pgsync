#!/bin/sh -e

set -x

black pgsync tests examples demo bin/* scripts
isort --profile black pgsync tests examples demo bin/* scripts
