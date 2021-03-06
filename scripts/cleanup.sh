#!/bin/sh -e

if [ -d 'build' ] ; then
    rm -r build
fi
if [ -d 'dist' ] ; then
    rm -r dist
fi
if [ -d 'wheels' ] ; then
    rm -r wheels
fi
if [ -d '.pytest_cache' ] ; then
    rm -r .pytest_cache
fi
if [ -d '__pycache__' ] ; then
    rm -r __pycache__
fi

rm -rf .coverage
rm -rf .coverage.*
rm -rf *.egg-info
rm -rf .eggs
rm -rf .mypy_cache
find . -type f -name ".*_*" -exec rm -r "{}" \;
find . -type d -name *.egg-info -exec rm -r "{}" \;
find . -type d -name __pycache__ -exec rm -r "{}" \;
find . -type f -name *.pyc -exec rm -r "{}" \;
find . -type f -name *.so -exec rm -r "{}" \;
find . -type f -name *.c -exec rm -r "{}" \;
