SET @ints_rows = 1000;

CREATE TABLE ints (
  i int primary key,
  v int COMMENT 'UNIFORM 0 @ints_rows'
) COMMENT = 'ROWS=@ints_rows';
