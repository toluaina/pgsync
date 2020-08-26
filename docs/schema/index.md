Schema definition file


```JSON
[
    {
        "database": "<Postgres database name>",
        "index": "<Elasticsearch index name>",
        "nodes": [
            {
                "table": "<root table name>",
                "schema": "<schema name>",
                "columns": [
                    "<column 1>",
                    "<column 2>",
                    ...
                ],
                "children": [
                    {
                        "table": "<child table name>",
                        "columns": [
                            "<column 1>",
                            "<column 2>",
                            ...
                        ],
                        "label": "<document label name>",
                        "relationship": {
                            "variant": "object" | "scalar",
                            "type": "one_to_one" | "one_to_many",
                            "through_tables": [
                                "<through table name>"
                            ]
                        },
                        "children": [],
                        "transform": {
                            "rename": {
                                "<old column 1>": "<new column 1>",
                                "<old column 2>": "<new column 2>",
                                ...
                            }
                        }
                    },
                    ...
                ]
            }
        ]
    }
]
```


## Document and node structure:

### `database`
This is the database name

### `index`
Optional Elasticsearch index (defaults to database name)

### `table`
Node table name

### `schema`
Optional Postgres table schema (defaults to public)

### `label`
Optional node name in Elasticsearch (defaults to table name)

### `columns`
Optional list of columns to display. This can be omitted in which case it selects all
columns.

### `children`
Optional list of child nodes if any.
This has the same structure as a parent node.

### `relationship`
Describes the relationship between parent and child.

- #### `variant`
variant can be `object` or `scalar`

    - #### `object`

        ```JSON
        {
            "name": "Oxford Press",
            "id": 1,
            "is_active": false
        }
        ```

    - #### `scalar`

        ```JSON
        ["Haruki Murakami", "Philip Gabriel"]
        ```

- #### `type`
type can be `one_to_one` or `one_to_many` depending on the relationship type between 
parent and child

- #### `through_tables`
This is the intermediate table that connects the parent to the child


### `transform`

This allows transforming some node properties.
For now, the only operation supported is the `rename` transform.

- #### `rename`
rename a node column

    ```JSON
        "rename": {
            "<old column name 1>": "<new column name 1>",
            "<old column name 2>": "<new column name 2>",
        }
    ```


!!! info
    Changing the schema effectively changes the structure of document in Elasticsearch 
    and this requires re-indexing Elasticsearch.

    See advanced section on re-indexing on how-to.
