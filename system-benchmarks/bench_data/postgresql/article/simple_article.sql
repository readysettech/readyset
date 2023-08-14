CREATE TABLE IF NOT EXISTS articles (
  id int NOT NULL PRIMARY KEY,
  author_id int NOT NULL,
  title varchar(128) NOT NULL,
  full_text text NOT NULL,
  priority int DEFAULT 1
);

COMMENT ON TABLE articles IS 'ROWS=100';
COMMENT ON COLUMN articles.author_id IS 'UNIFORM 0 100';
COMMENT ON COLUMN articles.title IS 'REGEX "[A-Za-z]{50,60}" UNIQUE';
COMMENT ON COLUMN articles.full_text IS 'REGEX "[A-Za-z0-9/]{64,128}"';
