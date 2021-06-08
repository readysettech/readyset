CREATE TABLE articles (
  id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  author_id int(11) NOT NULL,
  creation_time TIMESTAMP NOT NULL,
  keywords varchar(40) NOT NULL,
  title varchar(128) NOT NULL,
  full_text varchar(255) NOT NULL,
  short_text varchar(512) NOT NULL,
  image_url varchar(128) NOT NULL,
  url varchar(128) NOT NULL,
  type varchar(20) DEFAULT NULL,
  priority int(8) DEFAULT 1
);

CREATE TABLE authors (
  id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name varchar(40) NOT NULL,
  image_url varchar(128) NOT NULL
);

CREATE TABLE users (
  id int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY
);

CREATE TABLE recommendations (
  user_id int(11) NOT NULL,
  article_id int(11) NOT NULL
);
