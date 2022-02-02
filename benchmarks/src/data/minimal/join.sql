SELECT int_data FROM t1 LEFT JOIN t2 ON t1.uid = t2.t1_uid WHERE t2.uid = ?;
