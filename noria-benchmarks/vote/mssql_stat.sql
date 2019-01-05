SET NOCOUNT ON;
SELECT t.name, SUM(a.total_pages) * 8 AS size
FROM (
  SELECT name, object_id FROM sys.tables
  UNION
  SELECT name, object_id FROM sys.views
) t
INNER JOIN      
    sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN 
    sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN 
    sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY 
    t.Name;
GO
