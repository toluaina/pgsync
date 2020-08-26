Transform nodes allow changing the output of the document type


We can simply define this [JSON](https://jsonapi.org) schema.

```JSON
[
    {
        "database": "book",
        "index": "book",
        "nodes": [
            {
                "table": "book",
                "columns": [
                    "isbn",
                    "title"
                ],
                "transform": {
                    "rename": {
                        "isbn": "book_isbn",
                        "title": "book_title"
                    }
                }
            }
        ]
    }
]
```

To get this document structure in [Elasticsearch](https://www.elastic.co/products/elastic-stack)

```JSON
[
  {
      "book_isbn": "9785811243570",
      "book_title": "Charlie and the chocolate factory"
  },
  {
      "book_isbn": "9788374950978",
      "book_title": "Kafka on the Shore"
  },
  {
      "book_isbn": "9781471331435",
      "book_title": "1984"
  }
]
```

