CREATE TABLE title_basics (
    tconst text NOT NULL primary key,
    titletype text default 'movie',
    primarytitle text,
    originaltitle text,
    isadult boolean default true,
    startyear integer,
    endyear integer default null,
    runtimeminutes integer,
    genres text default 'Comedy,Crime,Sci-Fi'
);

COMMENT ON TABLE title_basics IS 'ROWS=400000';
COMMENT ON COLUMN title_basics.primarytitle IS 'REGEX "[A-Za-z]{15,64}"';
COMMENT ON COLUMN title_basics.originaltitle IS 'REGEX "[A-Za-z]{15,128}"';
COMMENT ON COLUMN title_basics.startyear IS 'UNIFORM 1900 2025';
COMMENT ON COLUMN title_basics.runtimeminutes IS 'UNIFORM 15 200';

