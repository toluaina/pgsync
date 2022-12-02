#!/bin/sh

for key in `echo 'KEYS user*' | redis-cli --scan --pattern '*' | awk '{print $1}'`
    do echo DEL $key
done | redis-cli
