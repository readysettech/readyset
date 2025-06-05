SET @num_rows = 10000000;
SET @num_groups = 50000;

CREATE TABLE decimals (
  id int primary key,
  group_id int COMMENT 'UNIFORM 0 @num_groups',
  value DECIMAL(27,10) COMMENT 'RANDOM'
) COMMENT = 'ROWS=@num_rows';
