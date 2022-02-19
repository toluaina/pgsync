#!/bin/sh -e

echo 'Cleanup.'

declare -a dirs=("build" "dist" "wheels" ".pytest_cache" "__pycache__")
for i in "${dirs[@]}"
do
    if [ -d "$i" ] ; then
        echo "Deleting: $i"
        rm -r "$i"
    fi
done

declare -a files=(".coverage" ".coverage.*" "*.egg-info" ".eggs" ".mypy_cache")
for i in "${files[@]}"
do
    if [ -f "$i" ] ; then
        echo "Deleting: $i"
        rm -r "$i"
    fi
done

declare -a dirs=("*.egg-info" "__pycache__")
for i in "${dirs[@]}"
do
    find . -type d -name "$i" -exec rm -r "{}" \;
done

declare -a files=(".*_*" "*.pyc" "*.so" "*.c ")
for i in "${files[@]}"
do
    find . -type f -name "$i"  -exec rm -r "{}" \;
done
