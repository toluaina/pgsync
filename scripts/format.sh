#!/bin/sh -e

set -x

isort pgsync tests examples demo bin/* scripts
black pgsync tests examples demo bin/* scripts
