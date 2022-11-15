CREATE TABLE t1 (
    uid INT(32) PRIMARY KEY,
    group1 INT(32) COMMENT 'group 10',
    group2 INT(32) COMMENT 'group 50',
    text_data varchar(1000) COMMENT 'REGEX [A-Za-z]{900,900}',
    int_data INT(2) DEFAULT 4
) COMMENT = 'ROWS=1000';

CREATE TABLE t2 (
    uid INT(32) PRIMARY KEY,
    t1_uid INT(32) COMMENT 'group 2'
) COMMENT = 'ROWS=1000';
