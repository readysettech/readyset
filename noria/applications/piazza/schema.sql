--
-- Table structure for table `Post`
--

CREATE TABLE `Post` (
  `p_id` int(11) NOT NULL auto_increment,
  `p_cid` int(11) NOT NULL,
  `p_author` int(11) NOT NULL,
  `p_content` text NOT NULL default '',
  `p_private` tinyint(1) NOT NULL default '0',
  `p_anonymous` tinyint(1) NOT NULL default '0',
  PRIMARY KEY  (`p_id`),
  UNIQUE KEY `p_id` (`p_id`),
  KEY `p_cid` (`p_cid`),
  KEY `p_author` (`p_author`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `User`
--

CREATE TABLE `User` (
  `u_id` int(11) NOT NULL auto_increment,
  PRIMARY KEY  (`u_id`),
  UNIQUE KEY `u_id` (`u_id`),
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `Class`
--

CREATE TABLE `Class` (
  `c_id` int(11) NOT NULL auto_increment,
  PRIMARY KEY  (`c_id`),
  UNIQUE KEY `c_id` (`c_id`),
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `Post`
--

CREATE TABLE `Role` (
  `r_uid` int(11) NOT NULL,
  `r_cid` int(11) NOT NULL,
  `r_role` tinyint(1) NOT NULL default '0',
  PRIMARY KEY  (`r_uid`, `r_cid`),
  KEY `r_uid` (`r_uid`),
  KEY `r_cid` (`r_cid`),
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
