SET @ints_rows = 500000;
SET @num_ints = 100;

CREATE TABLE ints (
  i int primary key,
  v int COMMENT 'UNIFORM 0 @num_ints'
) COMMENT = 'ROWS=@ints_rows';
