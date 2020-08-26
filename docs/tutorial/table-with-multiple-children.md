What if we added a new table `publisher`

**Publisher**

| id *(PK)* | name |
| ------------- | ------------- |
| 1 | Oxford Press |
| 2 | Penguin Books |
| 3 | Pearson Press |

and we added `publisher_id` as a foreign key to the Book table:


**Book**

| isbn *(PK)* | title | description | publisher_id (FK) |
| ------------- | ------------- | ------------- | ------------- |
| 9785811243570 | Charlie and the chocolate factory | Willy Wonka’s famous chocolate factory is opening at last! | 1 |
| 9788374950978 | Kafka on the Shore | Kafka on the Shore is a 2002 novel by Japanese author Haruki Murakami. | 2 |
| 9781471331435 | 1984 | 1984 was George Orwell’s chilling prophecy about the dystopian future. | 3 |


We can simply define this [JSON](https://jsonapi.org) schema where the **_book_** table is still the pivot table.

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
                            "name"
                        ]
                    },
                    {
                        "table": "publisher",
                        "columns": [
                            "name",
                            "id"
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
      "description": "Willy Wonka’s famous chocolate factory is opening at last!",
      "author": ["Roald Dahl"],
      "publisher": "Oxford Press"
  },
  {
      "isbn": "9788374950978",
      "title": "Kafka on the Shore",
      "description": "Kafka on the Shore is a 2002 novel by Japanese author Haruki Murakami",
      "author": ["Haruki Murakami", "Philip Gabriel"],
      "publisher": "Penguin Books"
  },
  {
      "isbn": "9781471331435",
      "title": "1984",
      "description": "1984 was George Orwell’s chilling prophecy about the dystopian future",
      "author": ["George Orwell"],
      "publisher": "Pearson Press"
  }
]
```
