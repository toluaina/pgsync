Postgres supports having multiple schemas in the same database.

You can select the schema in the node instead of the default `public` schema.


```JSON
[
    {
        "database": "book",
        "index": "book",
        "nodes": [
            {
                "table": "book",
                "schema": "my_book_library",
                "columns": [
                    "isbn",
                    "title"
                ]
            }
        ]
    }
]
```
