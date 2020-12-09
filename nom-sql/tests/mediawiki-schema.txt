-- SQL to create the initial tables for the Wikipedia database.
-- This is read and executed by the install script; you should
-- never have to run it by itself.
--
-- Only UNIQUE keys are defined here; the rest are added by
-- indexes.sql.
--

DROP TABLE IF EXISTS user;
CREATE TABLE user (
  user_id int(5) unsigned NOT NULL auto_increment,
  user_name varchar(255) binary NOT NULL default '',
  user_rights tinyblob NOT NULL default '',
  user_password tinyblob NOT NULL default '',
  user_newpassword tinyblob NOT NULL default '',
  user_email tinytext NOT NULL default '',
  user_options blob NOT NULL default '',  
  user_touched char(14) binary NOT NULL default '',
  UNIQUE KEY user_id (user_id)
) TYPE=MyISAM PACK_KEYS=1;
	
DROP TABLE IF EXISTS user_newtalk;
CREATE TABLE user_newtalk (
  user_id int(5) NOT NULL default '0',
  user_ip varchar(40) NOT NULL default ''
) TYPE=MyISAM;

DROP TABLE IF EXISTS cur;
CREATE TABLE cur (
  cur_id int(8) unsigned NOT NULL auto_increment,
  cur_namespace tinyint(2) unsigned NOT NULL default '0',
  cur_title varchar(255) binary NOT NULL default '',
  cur_text mediumtext NOT NULL default '',
  cur_comment tinyblob NOT NULL default '',
  cur_user int(5) unsigned NOT NULL default '0',
  cur_user_text varchar(255) binary NOT NULL default '',
  cur_timestamp char(14) binary NOT NULL default '',
  cur_restrictions tinyblob NOT NULL default '',
  cur_counter bigint(20) unsigned NOT NULL default '0',
  cur_is_redirect tinyint(1) unsigned NOT NULL default '0',
  cur_minor_edit tinyint(1) unsigned NOT NULL default '0',
  cur_is_new tinyint(1) unsigned NOT NULL default '0',
  cur_random real unsigned NOT NULL,
  cur_touched char(14) binary NOT NULL default '',
  inverse_timestamp char(14) binary NOT NULL default '',
  UNIQUE KEY cur_id (cur_id)
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS old;
CREATE TABLE old (
  old_id int(8) unsigned NOT NULL auto_increment,
  old_namespace tinyint(2) unsigned NOT NULL default '0',
  old_title varchar(255) binary NOT NULL default '',
  old_text mediumtext NOT NULL default '',
  old_comment tinyblob NOT NULL default '',
  old_user int(5) unsigned NOT NULL default '0',
  old_user_text varchar(255) binary NOT NULL,
  old_timestamp char(14) binary NOT NULL default '',
  old_minor_edit tinyint(1) NOT NULL default '0',
  old_flags tinyblob NOT NULL default '',
  inverse_timestamp char(14) binary NOT NULL default '',
  UNIQUE KEY old_id (old_id)
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS archive;
CREATE TABLE archive (
  ar_namespace tinyint(2) unsigned NOT NULL default '0',
  ar_title varchar(255) binary NOT NULL default '',
  ar_text mediumtext NOT NULL default '',
  ar_comment tinyblob NOT NULL default '',
  ar_user int(5) unsigned NOT NULL default '0',
  ar_user_text varchar(255) binary NOT NULL,
  ar_timestamp char(14) binary NOT NULL default '',
  ar_minor_edit tinyint(1) NOT NULL default '0',
  ar_flags tinyblob NOT NULL default ''
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS links;
CREATE TABLE links (
  l_from varchar(255) binary NOT NULL default '',
  l_to int(8) unsigned NOT NULL default '0'
) TYPE=MyISAM;

DROP TABLE IF EXISTS brokenlinks;
CREATE TABLE brokenlinks (
  bl_from int(8) unsigned NOT NULL default '0',
  bl_to varchar(255) binary NOT NULL default ''
) TYPE=MyISAM;

DROP TABLE IF EXISTS imagelinks;
CREATE TABLE imagelinks (
  il_from varchar(255) binary NOT NULL default '',
  il_to varchar(255) binary NOT NULL default ''
) TYPE=MyISAM;

DROP TABLE IF EXISTS site_stats;
CREATE TABLE site_stats (
  ss_row_id int(8) unsigned NOT NULL,
  ss_total_views bigint(20) unsigned default '0',
  ss_total_edits bigint(20) unsigned default '0',
  ss_good_articles bigint(20) unsigned default '0',
  UNIQUE KEY ss_row_id (ss_row_id)
) TYPE=MyISAM;

DROP TABLE IF EXISTS ipblocks;
CREATE TABLE ipblocks (
  ipb_address varchar(40) binary NOT NULL default '',
  ipb_user int(8) unsigned NOT NULL default '0',
  ipb_by int(8) unsigned NOT NULL default '0',
  ipb_reason tinyblob NOT NULL default '',
  ipb_timestamp char(14) binary NOT NULL default ''
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS image;
CREATE TABLE image (
  img_name varchar(255) binary NOT NULL default '',
  img_size int(8) unsigned NOT NULL default '0',
  img_description tinyblob NOT NULL default '',
  img_user int(5) unsigned NOT NULL default '0',
  img_user_text varchar(255) binary NOT NULL default '',
  img_timestamp char(14) binary NOT NULL default ''
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS oldimage;
CREATE TABLE oldimage (
  oi_name varchar(255) binary NOT NULL default '',
  oi_archive_name varchar(255) binary NOT NULL default '',
  oi_size int(8) unsigned NOT NULL default 0,
  oi_description tinyblob NOT NULL default '',
  oi_user int(5) unsigned NOT NULL default '0',
  oi_user_text varchar(255) binary NOT NULL default '',
  oi_timestamp char(14) binary NOT NULL default ''
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS random;
CREATE TABLE random (
  ra_current tinyint(1) unsigned not null default 0,
  ra_title varchar(255) binary not null default ''
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS recentchanges;
CREATE TABLE recentchanges (
  rc_timestamp varchar(14) binary NOT NULL default '',
  rc_cur_time varchar(14) binary NOT NULL default '',
  rc_user int(10) unsigned NOT NULL default '0',
  rc_user_text varchar(255) binary NOT NULL default '',
  rc_namespace tinyint(3) unsigned NOT NULL default '0',
  rc_title varchar(255) binary NOT NULL default '',
  rc_comment varchar(255) binary NOT NULL default '',
  rc_minor tinyint(3) unsigned NOT NULL default '0',
  rc_bot tinyint(3) unsigned NOT NULL default '0',
  rc_new tinyint(3) unsigned NOT NULL default '0',
  rc_cur_id int(10) unsigned NOT NULL default '0',
  rc_this_oldid int(10) unsigned NOT NULL default '0',
  rc_last_oldid int(10) unsigned NOT NULL default '0'
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS watchlist;
CREATE TABLE watchlist (
  wl_user int(5) unsigned NOT NULL,
  wl_namespace tinyint(2) unsigned NOT NULL default '0',
  wl_title varchar(255) binary NOT NULL default '',
  UNIQUE KEY (wl_user, wl_namespace, wl_title)
) TYPE=MyISAM PACK_KEYS=1;

DROP TABLE IF EXISTS math;
CREATE TABLE math (
    math_inputhash varchar(16) NOT NULL,
    math_outputhash varchar(16) NOT NULL,
    math_html_conservativeness tinyint(1) NOT NULL,
    math_html text,
    math_mathml text,
    UNIQUE KEY math_inputhash (math_inputhash)
) TYPE=MyISAM;

-- Table searchindex must be MyISAM for fulltext support

DROP TABLE IF EXISTS searchindex;
CREATE TABLE searchindex (
  si_page int(8) unsigned NOT NULL,
  si_title varchar(255) NOT NULL default '',
  si_text mediumtext NOT NULL default '',
  UNIQUE KEY (si_page)
) TYPE=MyISAM PACK_KEYS=1;

