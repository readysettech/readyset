CREATE TABLE `title_basics` (
  `tconst` varchar(255) NOT NULL primary key,
  `titletype` varchar(255) default 'movie',
  `primarytitle` varchar(255) COMMENT 'REGEX "[A-Za-z]{15,64}"',
  `originaltitle` varchar(255) COMMENT 'REGEX "[A-Za-z]{15,128}"',
  `isadult` tinyint(1) DEFAULT 0,
  `startyear` int COMMENT 'UNIFORM 1900 2025',
  `runtimeminutes` int COMMENT 'UNIFORM 15 200',
  `genres` varchar(255) default 'Comedy,Crime,Sci-Fi'
) COMMENT = 'ROWS=400000';
