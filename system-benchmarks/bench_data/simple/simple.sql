SET @users_rows = 100000;

CREATE TABLE users (
  id int NOT NULL PRIMARY KEY,
  name text NOT NULL
) COMMENT = 'ROWS = @users_rows';
