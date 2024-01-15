-- Here is a great way to search a cluster for particular column name or table

SELECT c.column_name,
       c.table_schema,
       c.table_name,
       'SELECT ' || c.column_name || ', * FROM ' || table_schema || '.' || table_name || ' LIMIT 100' AS sql
FROM information_schema.columns AS c
WHERE LOWER(c.column_name || c.table_name) SIMILAR TO '%wildcard here%'
ORDER BY c.table_schema,
         c.table_name;