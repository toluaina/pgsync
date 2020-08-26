Re-indexing involves:

- Deleting the Elasticsearch index
  ```
  curl -X DELETE <protocol>://<hostname>:<port>/<index>
  ```
- Delete the checkpoint file.  
  This is a hidden file which is a concatenation of the database name and the index name
  ```
  rm .<database name>_<index name>
  ```
- Re-run pgsync
  ```
  pgsync
  ```


!!! info
    If any new tables are added or removed from the schema you should re-run bootstrap before re-running pgsync again.
