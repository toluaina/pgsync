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

rm -rf .coverge.*
rm -rf *.egg-info
find . -type f -name .book* -exec rm -r "{}" \;
find . -type f -name .social* -exec rm -r "{}" \;
find . -type f -name .airbnb* -exec rm -r "{}" \;
find . -type f -name .airline* -exec rm -r "{}" \;
find . -type f -name .movie* -exec rm -r "{}" \;
find . -type f -name .quiz* -exec rm -r "{}" \;
find . -type f -name .rental* -exec rm -r "{}" \;
find . -type f -name .shakespeare* -exec rm -r "{}" \;
find . -type f -name .survey* -exec rm -r "{}" \;
find . -type d -name *.egg-info -exec rm -r "{}" \;
find . -type d -name __pycache__ -exec rm -r "{}" \;
find . -type f -name *.pyc -exec rm -r "{}" \;
find . -type f -name *.so -exec rm -r "{}" \;
find . -type f -name *.c -exec rm -r "{}" \;
