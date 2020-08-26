# PGSync

<p align="center">
    <em>Postgres to Elasticsearch sync</em>
</p>
<p align="center">
<a href="https://github.com/toluaina/pg-sync/actions" target="_blank">
    <img src="https://github.com/toluaina/pg-sync/workflows/build/badge.svg" alt="Build">
</a>
</a>
<a href="https://badge.fury.io/py/pgsync" target="_blank">
    <img src="https://badge.fury.io/py/pgsync.svg" alt="Package version">
</a>
</p>


---

**Documentation**: <a href="https://pgsync.com" target="_blank">https://pgsync.com</a>

**Source Code**: <a href="https://github.com/toluaina/pg-sync" target="_blank">https://github.com/toluaina/pg-sync</a>

---


PGSync is a middleware for syncing data from Postgres to Elasticsearch. 
It allows you to keep Postgres as your source of truth data source and expose 
structured denormalized documents in Elasticsearch.

Simply describe your document structure or schema in JSON and PGSync will 
continuously capture changes in your data and load it into Elasticsearch without 
writing any code. PGSync transforms your relational data into a structured 
document format.

It allows you to take advantage of the expressive power and scalability of 
Elasticsearch directly from Postgres. You don’t have to write complex queries 
and transformation pipelines. PGSync is lightweight, fast and flexible.
