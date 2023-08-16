create table users (
    id int not null primary key,
    username text
);

COMMENT ON TABLE users IS 'ROWS=100000';
COMMENT ON COLUMN users.username IS 'REGEX "[A-Za-z]{15,60}"';

create table stories (
    id int not null primary key,
    author int,
    title text,
    url text
);

COMMENT ON TABLE stories IS 'ROWS=10000';
COMMENT ON COLUMN stories.author IS 'UNIFORM 0 1000';
COMMENT ON COLUMN stories.title IS 'REGEX "[A-Za-z]{15,100}"';
COMMENT ON COLUMN stories.url IS 'REGEX "[A-Za-z]{15,60}"';

-- the index field is a bit of a hack to avoid duplicate key errors
-- in the data generator if we had the user/storyids in the PK.
create table votes (
    user_id int,
    story_id int
);

--CREATE INDEX votes_user_story_idx ON votes (user_id, story_id);

COMMENT ON TABLE votes IS 'ROWS=1200000';
COMMENT ON COLUMN votes.user_id IS 'UNIFORM 0 100000';
COMMENT ON COLUMN votes.story_id IS 'UNIFORM 0 10000';
