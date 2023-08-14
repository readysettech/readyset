CREATE TABLE IF NOT EXISTS articles (
  id int NOT NULL PRIMARY KEY,
  author_id int NOT NULL COMMENT 'UNIFORM 0 100',
  title varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z]{50,60}" UNIQUE',
  full_text text NOT NULL COMMENT 'REGEX "[A-Za-z0-9/]{64,128}"',
  priority int DEFAULT 1
) COMMENT='ROWS=100';
