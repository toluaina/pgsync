#!/bin/bash

pip install --upgrade pip-tools
pip-compile --output-file=requirements/dev.txt requirements/dev.in --upgrade
pip-compile --output-file=requirements/prod.txt requirements/prod.in --upgrade
