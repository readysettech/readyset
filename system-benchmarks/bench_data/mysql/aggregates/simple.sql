SET @ints_rows = 50000;
SET @num_ints = 97;


CREATE TABLE ints (
  i int primary key,
  v int COMMENT 'UNIFORM 0 @num_ints'
) COMMENT = 'ROWS=@ints_rows';
