#!/usr/bin/env bash

indices=(
        "new_notes",
        "new_client_relationship",
        "new_workstreams",
        "new_milestones",
        "new_projectteam",
        "new_files",
        "new_tasks"
        )

for index in ${indices[*]}
do
    curl -X DELETE ${ELASTICSEARCH_SCHEME}://${ELASTICSEARCH_HOST}:${ELASTICSEARCH_PORT}/${index}
done

bootstrap

pgsync
