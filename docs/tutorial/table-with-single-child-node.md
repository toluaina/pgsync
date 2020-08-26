Consider this example of a Book library database.

**Book**

| isbn *(PK)* | title | description |
| ------------- | ------------- | ------------- |
| 9785811243570 | Charlie and the chocolate factory | Willy Wonka’s famous chocolate factory is opening at last! |
| 9788374950978 | Kafka on the Shore | Kafka on the Shore is a 2002 novel by Japanese author Haruki Murakami. |
| 9781471331435 | 1984 | 1984 was George Orwell’s chilling prophecy about the dystopian future. |

**Author**

| id *(PK)* | name |
| ------------- | ------------- |
| 1 | Roald Dahl |
| 2 | Haruki Murakami |
| 3 | Philip Gabriel |
| 4 | George Orwell |

**BookAuthor**

| id *(PK)* | book_isbn *(FK)* | author_id *(FK)* |
| -- | ------------- | ---------- |
| 1 | 9785811243570 | 1 |
| 2 | 9788374950978 | 2 |
| 3 | 9788374950978 | 3 |
| 4 | 9781471331435 | 4 |

With PGSync, we can simply define this [JSON](https://jsonapi.org) schema where the **_book_** table is the pivot.
A **_pivot_** table indicates the root of your document.

```JSON
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
        }
    ]
}
```

To get this document structure in [Elasticsearch](https://www.elastic.co/products/elastic-stack)

```JSON
[
  {
      "isbn": "9785811243570",
      "title": "Charlie and the chocolate factory",
      "description": "Willy Wonka’s famous chocolate factory is opening at last!",
      "author": ["Roald Dahl"]
  },
  {
      "isbn": "9788374950978",
      "title": "Kafka on the Shore",
      "description": "Kafka on the Shore is a 2002 novel by Japanese author Haruki Murakami",
      "author": ["Haruki Murakami", "Philip Gabriel"]
  },
  {
      "isbn": "9781471331435",
      "title": "1984",
      "description": "1984 was George Orwell’s chilling prophecy about the dystopian future",
      "author": ["George Orwell"]
  }
]
```

Behind the scenes, PGSync is generating advanced queries for you such as.

```SQL
SELECT 
       JSON_BUILD_OBJECT(
          'isbn', book_1.isbn, 
          'title', book_1.title, 
          'description', book_1.description,
          'authors', anon_1.authors
       ) AS "JSON_BUILD_OBJECT_1",
       book_1.id
FROM book AS book_1
LEFT OUTER JOIN
  (SELECT 
          JSON_AGG(anon_2.anon) AS authors,
          book_author_1.book_isbn AS book_isbn
   FROM book_author AS book_author_1
   LEFT OUTER JOIN
     (SELECT 
             author_1.name AS anon,
             author_1.id AS id
      FROM author AS author_1) AS anon_2 ON anon_2.id = book_author_1.author_id
   GROUP BY book_author_1.book_isbn) AS anon_1 ON anon_1.book_isbn = book_1.isbn
```
