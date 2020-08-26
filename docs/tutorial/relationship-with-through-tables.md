Each child node can have only one direct parent.

We can specify the relationship between a parent and child node with the `relationship` property.

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
                    "title",
                    "description"
                ],
                "children": [
                    {
                        "table": "author",
                        "columns": [
                            "id", "name"
                        ],
                        "relationship": {
                            "type": "one_to_many",
                            "variant": "object",
                            "through_tables": [
                                "book_author"
                            ]
                        }
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
      "author": [
          {
            "id": 1,
            "name": "Roald Dahl"
          }
       ]
  },
  {
      "isbn": "9788374950978",
      "title": "Kafka on the Shore",
      "author": [
          {
            "id": 2,
            "name": "Haruki Murakami"
          },
          {
            "id": 3,
            "name": "Philip Gabriel"
          }
       ]
  },
  {
      "isbn": "9781471331435",
      "title": "1984",
      "author": [
          {
            "id": 4,
            "name": "George Orwell"
          }
       ]
  }
]
```

!!! info
    A relationship must define both a `variant` and a `type` attribute

