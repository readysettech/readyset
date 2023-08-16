SET @users_rows = 100000;
SET @stories_rows = 10000;
SET @authors_cnt = 1000;
SET @votes_rows = 1200000;

create table users (
    id int primary key,
    username text COMMENT 'REGEX "[A-Za-z]{15,60}"'
) COMMENT = 'ROWS=@users_rows';

create table stories (
    id int primary key,
    author int COMMENT 'UNIFORM 0 1000',
    title text COMMENT 'REGEX "[A-Za-z]{15,100}"',
    url text COMMENT 'REGEX "[A-Za-z]{15,60}"'
) COMMENT = 'ROWS=@stories_rows';


create table votes (
    user_id int COMMENT 'UNIFORM 0 @users_rows',
    story_id int COMMENT 'UNIFORM 0 @stories_rows'
) COMMENT = 'ROWS=@votes_rows';

-- INDEX!
