SELECT
table_schema as `Database`,
table_name AS `Table`,
round(((data_length + index_length) / 1024), 2) `kB`
FROM information_schema.TABLES
WHERE table_schema = 'soup'
ORDER BY (data_length + index_length) DESC;
