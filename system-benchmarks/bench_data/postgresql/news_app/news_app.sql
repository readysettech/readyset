CREATE TABLE articles (
  id int NOT NULL PRIMARY KEY,
  author_id int NOT NULL,
  creation_time TIMESTAMP NOT NULL DEFAULT NOW(),
  keywords text NOT NULL,
  title text NOT NULL,
  full_text text NOT NULL,
  short_text text NOT NULL,
  image_url text NOT NULL,
  url text NOT NULL,
  type text DEFAULT NULL,
  priority int DEFAULT 1
);

COMMENT ON TABLE articles IS 'ROWS=10000';
COMMENT ON COLUMN articles.author_id IS 'UNIFORM 0 100';
COMMENT ON COLUMN articles.keywords IS 'REGEX "[A-Za-z]{10,40}"';
COMMENT ON COLUMN articles.title IS 'REGEX "[A-Za-z]{15,60}" UNIQUE';
COMMENT ON COLUMN articles.image_url IS 'REGEX "[A-Za-z0-9/]{20,80}"';
COMMENT ON COLUMN articles.url IS 'REGEX "[A-Za-z0-9/]{20,80}"';

CREATE TABLE authors (
  id int NOT NULL PRIMARY KEY,
  name text NOT NULL,
  image_url text NOT NULL
);

COMMENT ON TABLE authors IS 'ROWS=100';
COMMENT ON COLUMN authors.name IS 'REGEX "[A-Z][a-z]{2,12}[A-Z][a-z]{2,20}" UNIQUE';
COMMENT ON COLUMN authors.image_url IS 'REGEX "[A-Za-z0-9/]{20,80}"';

CREATE TABLE users (
  id int NOT NULL PRIMARY KEY
);

COMMENT ON TABLE users IS 'ROWS=100000';

CREATE TABLE recommendations (
  user_id int NOT NULL,
  article_id int NOT NULL,
  PRIMARY KEY (user_id, article_id)
);

COMMENT ON TABLE recommendations IS 'ROWS=1000000';
COMMENT ON COLUMN recommendations.user_id IS 'UNIFORM 0 100000';
COMMENT ON COLUMN recommendations.article_id IS 'UNIFORM 0 10000';
