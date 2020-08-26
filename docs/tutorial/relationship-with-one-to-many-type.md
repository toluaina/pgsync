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
                "children": [
                    {
                        "table": "author",
                        "columns": [
                            "id",
                            "name"
                        ],
                        "relationship": {
                            "variant": "object",
                            "type": "one_to_many"
                        },
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
      "publisher": [
          {
            "id": 1,
            "name": "Oxford Press"
          }
       ]
  },
  {
      "isbn": "9788374950978",
      "title": "Kafka on the Shore",
      "publisher": [
          {
            "id": 2,
            "name": "Penguin Books"
          }
       ]
  },
  {
      "isbn": "9781471331435",
      "title": "1984",
      "publisher": [
          {
            "id": 3,
            "name": "Pearson Press"
          }
       ]
  }
]
```

!!! info
    A relationship must define both a `variant` and a `type` attribute

