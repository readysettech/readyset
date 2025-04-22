SET @users_rows = 10000;
SET @posts_rows = 100000;

DROP TABLE IF EXISTS users;

CREATE TABLE users (
  id int NOT NULL PRIMARY KEY,
  name text NOT NULL COLLATE latin1_swedish_ci COMMENT 'CHARS 10 100 latin1'
) COMMENT = 'ROWS=@users_rows';

DROP TABLE IF EXISTS posts;

CREATE TABLE posts (
  id int NOT NULL PRIMARY KEY,
  users_id int COMMENT 'UNIFORM 0 @users_rows',
  title text NOT NULL COLLATE latin1_swedish_ci COMMENT 'CHARS 10 100 latin1',
  content longtext NOT NULL COLLATE latin1_swedish_ci COMMENT 'CHARS 1000 100000 latin1'
) COMMENT = 'ROWS=@posts_rows';
