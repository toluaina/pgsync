Labels are used to control the output of the document node.


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
                "children": [
                    {
                        "table": "author",
                        "label": "authors",
                        "columns": [
                            "name"
                        ]
                    }
                ]
            }
        ]
    }
]
```

To get this document structure in [Elasticsearch](https://www.elastic.co/products/elastic-stack)

```JSON
[
  {
      "isbn": "9785811243570",
      "title": "Charlie and the chocolate factory",
      "authors": ["Roald Dahl"]
  },
  {
      "isbn": "9788374950978",
      "title": "Kafka on the Shore",
      "authors": ["Haruki Murakami", "Philip Gabriel"]
  },
  {
      "isbn": "9781471331435",
      "title": "1984",
      "authors": ["George Orwell"]
  }
]
```

