#!/bin/bash

pip-compile --output-file=requirements/dev.txt requirements/dev.in --upgrade
pip-compile --output-file=requirements/prod.txt requirements/prod.in --upgrade
pip-compile --output-file=requirements/test.txt requirements/test.in --upgrade
