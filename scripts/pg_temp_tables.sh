#!/bin/sh

source .env

export PGPASSWORD=$PG_PASSWORD

psql -U $PG_USER -h $PG_HOST -d postgres <<EOF
SELECT 
    datname AS "Database",
    temp_files AS "Temporary files",
    PG_SIZE_PRETTY(temp_bytes) AS "Size of temporary files"
FROM pg_stat_database db
EOF
