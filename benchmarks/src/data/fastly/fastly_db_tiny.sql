SET @articles_rows = 100;
SET @authors_rows = 10;
SET @users_rows = 100;
SET @recommendations_rows = 100;

CREATE TABLE articles (
  id int(11) NOT NULL PRIMARY KEY,
  author_id int(11) NOT NULL COMMENT 'UNIFORM 0 @authors_rows',
  creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  keywords varchar(40) NOT NULL COMMENT 'REGEX "[A-Za-z]{10,40}"',
  title varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z]{15,60}" UNIQUE',
  full_text varchar(255) NOT NULL,
  short_text varchar(512) NOT NULL,
  image_url varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z0-9/]{20,80}"',
  url varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z0-9/]{20,80}"',
  type varchar(20) DEFAULT NULL,
  priority int(8) DEFAULT 1
) COMMENT = 'ROWS=@articles_rows';

CREATE TABLE authors (
  id int(11) NOT NULL PRIMARY KEY,
  name varchar(40) NOT NULL COMMENT 'REGEX "[A-Z][a-z]{2,12}[A-Z][a-z]{2,20}" UNIQUE',
  image_url varchar(128) NOT NULL COMMENT 'REGEX "[A-Za-z0-9/]{20,80}"'
) COMMENT = 'ROWS=@authors_rows';

CREATE TABLE users (
  id int(11) NOT NULL PRIMARY KEY
) COMMENT = 'ROWS=@users_rows';

CREATE TABLE recommendations (
  user_id int(11) NOT NULL COMMENT 'UNIFORM 0 @users_rows',
  article_id int(11) NOT NULL COMMENT 'UNIFORM 0 @articles_rows'
) COMMENT = 'ROWS=@recommendations_rows';
