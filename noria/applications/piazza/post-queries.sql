CREATE CACHE posts FROM select * from Post where p_cid = ?;
-- CREATE CACHE post_count FROM select p_author, count(p_id) from Post group by p_author;
