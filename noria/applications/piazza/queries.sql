CREATE CACHE posts FROM select * from Post where p_cid=?;
-- public_posts: select * from Post WHERE Post.p_private=0 and p_cid=?;
-- private_posts: select * from Post WHERE Post.p_private=1 and p_cid=?;
-- tas: select * from Role WHERE r_role = 1 and r_cid=?;
-- ta_posts: select * from Post WHERE Post.p_author in (select r_uid from Role WHERE Role.r_role=1) and p_cid=?;
-- enrolled_in: select r_cid from Role where r_uid=?;
-- enrolled_students: select r_uid from Role where r_cid=?;
-- vote_count: select postId, COUNT(userId) AS votes FROM Vote GROUP BY postId;
