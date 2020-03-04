QUERY PaperList: SELECT Paper.*, Paper.title AS latest_version_title
           FROM Paper
           JOIN (SELECT *
                 FROM PaperVersion
                 ORDER BY PaperVersion.time
                 LIMIT 1)
                AS LatestPaperVersion
           ON (Paper.id = LatestPaperVersion.paper);
