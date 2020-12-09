CREATE TABLE UserProfile (
    username varchar(1024),
    email varchar(1024),
    name varchar(1024),
    affiliation varchar(1024),
    acm_number varchar(1024),
    level varchar(12),
    PRIMARY KEY (username)
);

CREATE TABLE UserPCConflict (
    username varchar(1024),
    pc varchar(1024),
    KEY username (username)
);

CREATE TABLE Paper (
    id int,
    author varchar(1024),
    accepted tinyint(1),
    PRIMARY KEY (id)
);

CREATE TABLE PaperPCConflict (
    paper int,
    pc varchar(1024),
    KEY paper (paper)
);

CREATE TABLE PaperCoauthor (
    paper int,
    author varchar(1024),
    KEY paper (paper)
);

CREATE TABLE ReviewAssignment (
    paper int,
    username varchar(1024),
    assign_type varchar(8),
    KEY paper (paper)
);

CREATE TABLE PaperVersion (
    paper int,
    title varchar(1024),
    -- N.B. is of type "file" in Jeeves --
    contents varchar(1024),
    abstract text,
    time datetime DEFAULT CURRENT_TIMESTAMP,
    KEY paper (paper)
);

CREATE TABLE Tag (
    name varchar(32)
    paper int,
    KEY paper (paper),
    KEY name (name)
);

CREATE TABLE Review (
    time datetime DEFAULT CURRENT_TIMESTAMP,
    paper int,
    reviewer varchar(1024),
    contents text,
    score_novelty int,
    score_presentation int,
    score_technical int,
    score_confidence int,
    KEY paper (paper)
);

CREATE TABLE Comment (
    time datetime DEFAULT CURRENT_TIMESTAMP,
    paper int,
    username varchar(1024),
    contents text,
    KEY paper (paper)
);
