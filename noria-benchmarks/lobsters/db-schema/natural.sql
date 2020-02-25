DROP TABLE IF EXISTS `comments` CASCADE;
CREATE TABLE `comments` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime, `short_id` varchar(10) DEFAULT '' NOT NULL, `story_id` int unsigned NOT NULL, `user_id` int unsigned NOT NULL, `parent_comment_id` int unsigned, `thread_id` int unsigned, `comment` mediumtext NOT NULL, `markeddown_comment` mediumtext, `is_deleted` tinyint(1) DEFAULT 0, `is_moderated` tinyint(1) DEFAULT 0, `is_from_email` tinyint(1) DEFAULT 0, `hat_id` int, fulltext INDEX `index_comments_on_comment`  (`comment`),  INDEX `confidence_idx`  (`confidence`), UNIQUE INDEX `short_id`  (`short_id`),  INDEX `story_id_short_id`  (`story_id`, `short_id`),  INDEX `thread_id`  (`thread_id`),  INDEX `index_comments_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `hat_requests` CASCADE;
CREATE TABLE `hat_requests` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `updated_at` datetime, `user_id` int, `hat` varchar(255) COLLATE utf8mb4_general_ci, `link` varchar(255) COLLATE utf8mb4_general_ci, `comment` text COLLATE utf8mb4_general_ci) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `hats` CASCADE;
CREATE TABLE `hats` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `updated_at` datetime, `user_id` int, `granted_by_user_id` int, `hat` varchar(255) NOT NULL, `link` varchar(255) COLLATE utf8mb4_general_ci, `modlog_use` tinyint(1) DEFAULT 0, `doffed_at` datetime) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `hidden_stories` CASCADE;
CREATE TABLE `hidden_stories` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int, `story_id` int, UNIQUE INDEX `index_hidden_stories_on_user_id_and_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `invitation_requests` CASCADE;
CREATE TABLE `invitation_requests` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `code` varchar(255), `is_verified` tinyint(1) DEFAULT 0, `email` varchar(255), `name` varchar(255), `memo` text, `ip_address` varchar(255), `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `invitations` CASCADE;
CREATE TABLE `invitations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int, `email` varchar(255), `code` varchar(255), `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `memo` mediumtext) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `keystores` CASCADE;
CREATE TABLE `keystores` (`key` varchar(50) DEFAULT '' NOT NULL, `value` bigint, PRIMARY KEY (`key`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `messages` CASCADE;
CREATE TABLE `messages` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `author_user_id` int unsigned, `recipient_user_id` int unsigned, `has_been_read` tinyint(1) DEFAULT 0, `subject` varchar(100), `body` mediumtext, `short_id` varchar(30), `deleted_by_author` tinyint(1) DEFAULT 0, `deleted_by_recipient` tinyint(1) DEFAULT 0, UNIQUE INDEX `random_hash`  (`short_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `moderations` CASCADE;
CREATE TABLE `moderations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `moderator_user_id` int, `story_id` int, `comment_id` int, `user_id` int, `action` mediumtext, `reason` mediumtext, `is_from_suggestions` tinyint(1) DEFAULT 0,  INDEX `index_moderations_on_created_at`  (`created_at`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `read_ribbons` CASCADE;
CREATE TABLE `read_ribbons` (`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, `is_following` tinyint(1) DEFAULT 1, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` bigint, `story_id` bigint,  INDEX `index_read_ribbons_on_story_id`  (`story_id`),  INDEX `index_read_ribbons_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `saved_stories` CASCADE;
CREATE TABLE `saved_stories` (`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` int, `story_id` int, UNIQUE INDEX `index_saved_stories_on_user_id_and_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `stories` CASCADE;
-- XXX: stories doesn't usually have an always-NULL column
CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `suggested_taggings` CASCADE;
CREATE TABLE `suggested_taggings` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int, `tag_id` int, `user_id` int) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `suggested_titles` CASCADE;
CREATE TABLE `suggested_titles` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int, `user_id` int, `title` varchar(150) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `tag_filters` CASCADE;
CREATE TABLE `tag_filters` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` int, `tag_id` int,  INDEX `user_tag_idx`  (`user_id`, `tag_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `taggings` CASCADE;
CREATE TABLE `taggings` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int unsigned NOT NULL, `tag_id` int unsigned NOT NULL, UNIQUE INDEX `story_id_tag_id`  (`story_id`, `tag_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `tags` CASCADE;
-- XXX: hotness_mod should be a float(24)
CREATE TABLE `tags` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `tag` varchar(25) DEFAULT '' NOT NULL, `description` varchar(100), `privileged` tinyint(1) DEFAULT 0, `is_media` tinyint(1) DEFAULT 0, `inactive` tinyint(1) DEFAULT 0, `hotness_mod` int DEFAULT 0, UNIQUE INDEX `tag`  (`tag`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `users` CASCADE;
-- CREATE TABLE `users` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `email` varchar(100) COLLATE utf8mb4_general_ci, `password_digest` varchar(75) COLLATE utf8mb4_general_ci, `created_at` datetime, `is_admin` tinyint(1) DEFAULT 0, `password_reset_token` varchar(75) COLLATE utf8mb4_general_ci, `session_token` varchar(75) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL, `about` mediumtext COLLATE utf8mb4_general_ci, `invited_by_user_id` int, `is_moderator` tinyint(1) DEFAULT 0, `pushover_mentions` tinyint(1) DEFAULT 0, `rss_token` varchar(75) COLLATE utf8mb4_general_ci, `mailing_list_token` varchar(75) COLLATE utf8mb4_general_ci, `mailing_list_mode` int DEFAULT 0, `karma` int DEFAULT 0 NOT NULL, `banned_at` datetime, `banned_by_user_id` int, `banned_reason` varchar(200) COLLATE utf8mb4_general_ci, `deleted_at` datetime, `disabled_invite_at` datetime, `disabled_invite_by_user_id` int, `disabled_invite_reason` varchar(200), `settings` text,  INDEX `mailing_list_enabled`  (`mailing_list_mode`), UNIQUE INDEX `mailing_list_token`  (`mailing_list_token`), UNIQUE INDEX `password_reset_token`  (`password_reset_token`), UNIQUE INDEX `rss_token`  (`rss_token`), UNIQUE INDEX `session_hash`  (`session_token`), UNIQUE INDEX `username`  (`username`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `users` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `karma` int DEFAULT 0 NOT NULL, UNIQUE INDEX `username`  (`username`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `votes` CASCADE;
CREATE TABLE `votes` (`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int unsigned NOT NULL, `story_id` int unsigned NOT NULL, `comment_id` int unsigned, `vote` tinyint NOT NULL, `reason` varchar(1),  INDEX `index_votes_on_comment_id`  (`comment_id`),  INDEX `user_id_comment_id`  (`user_id`, `comment_id`),  INDEX `user_id_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-----------------------------------------------------
-- Make views for all the computed columns
CREATE VIEW `FULL_story_tag_score` AS
SELECT taggings.story_id AS id, SUM(tags.hotness_mod) AS score
 FROM taggings
 JOIN tags ON (tags.id = taggings.tag_id)
GROUP BY taggings.story_id;

-- Comment score tracking
CREATE VIEW comment_upvotes AS SELECT votes.comment_id, votes.user_id FROM votes WHERE votes.story_id IS NULL AND votes.vote = 1;
CREATE VIEW comment_downvotes AS SELECT votes.comment_id, votes.user_id FROM votes WHERE votes.story_id IS NULL AND votes.vote = 0;

CREATE VIEW FULL_comment_upvotes AS
SELECT comment_upvotes.comment_id AS id, COUNT(*) as votes
FROM comment_upvotes GROUP BY comment_upvotes.comment_id;

CREATE VIEW FULL_comment_downvotes AS
SELECT comment_downvotes.comment_id AS id, COUNT(*) as votes
FROM comment_downvotes GROUP BY comment_downvotes.comment_id;

CREATE VIEW comment_votes AS
(SELECT FULL_comment_upvotes.id, FULL_comment_upvotes.votes AS score FROM FULL_comment_upvotes)
UNION
(SELECT FULL_comment_downvotes.id, 0 - FULL_comment_downvotes.votes AS score FROM FULL_comment_downvotes);

CREATE VIEW FULL_comment_score AS
SELECT comment_votes.id, SUM(comment_votes.score) as score
FROM comment_votes GROUP BY comment_votes.id;

-- Story score tracking
CREATE VIEW story_upvotes AS SELECT votes.story_id, votes.user_id FROM votes WHERE votes.comment_id IS NULL AND votes.vote = 1;
CREATE VIEW story_downvotes AS SELECT votes.story_id, votes.user_id FROM votes WHERE votes.comment_id IS NULL AND votes.vote = 0;

CREATE VIEW FULL_story_upvotes AS
SELECT story_upvotes.story_id AS id, COUNT(*) as votes
FROM story_upvotes GROUP BY story_upvotes.story_id;

CREATE VIEW FULL_story_downvotes AS
SELECT story_downvotes.story_id AS id, COUNT(*) as votes
FROM story_downvotes GROUP BY story_downvotes.story_id;

CREATE VIEW story_votes AS
(SELECT FULL_story_upvotes.id, FULL_story_upvotes.votes AS score FROM FULL_story_upvotes)
UNION
(SELECT FULL_story_downvotes.id, 0 - FULL_story_downvotes.votes AS score FROM FULL_story_downvotes);

CREATE VIEW FULL_story_score AS
SELECT story_votes.id, SUM(story_votes.score) as score
FROM story_votes GROUP BY story_votes.id;

-- Useful intermediate views
CREATE VIEW `comment_with_votes` AS
SELECT comments.*,
       FULL_comment_upvotes.votes AS upvotes,
       FULL_comment_downvotes.votes AS downvotes,
       FULL_comment_upvotes.votes - FULL_comment_downvotes.votes AS score,
FROM comments
LEFT JOIN FULL_comment_upvotes ON (comments.id = FULL_comment_upvotes.id)
LEFT JOIN FULL_comment_downvotes ON (comments.id = FULL_comment_downvotes.id);

CREATE VIEW `story_with_votes` AS
SELECT stories.*,
       FULL_story_upvotes.votes AS upvotes,
       FULL_story_downvotes.votes AS downvotes,
       FULL_story_upvotes.votes - FULL_story_downvotes.votes AS score,
FROM stories
LEFT JOIN FULL_story_upvotes ON (stories.id = FULL_story_upvotes.id)
LEFT JOIN FULL_story_downvotes ON (stories.id = FULL_story_downvotes.id);

-- Hotness computation
-- XXX: bah.. pretty sad that this join will end up full...
CREATE VIEW FULL_non_author_comments AS
SELECT comments.id, comments.story_id
  FROM comments
  JOIN stories ON (comments.story_id = stories.id)
 WHERE comments.user_id <> stories.user_id;

CREATE VIEW FULL_story_comment_score AS
SELECT FULL_non_author_comments.story_id AS id,
       SUM(FULL_comment_score.score) AS score
FROM FULL_non_author_comments
JOIN FULL_comment_score ON (FULL_comment_score.id = FULL_non_author_comments.id)
GROUP BY FULL_non_author_comments.story_id;

CREATE VIEW FULL_merged_story_score AS
SELECT stories.merged_story_id AS id, FULL_story_score.score
FROM FULL_story_score
JOIN stories ON (FULL_story_score.id = stories.id);

-- XXX: *technically* tag_score should be a multiplier
CREATE VIEW all_hotness_components AS
(SELECT FULL_story_tag_score.id, FULL_story_tag_score.score FROM FULL_story_tag_score)
UNION
(SELECT FULL_story_score.id, FULL_story_score.score FROM FULL_story_score)
UNION
(SELECT FULL_merged_story_score.id, FULL_merged_story_score.score FROM FULL_merged_story_score)
UNION
(SELECT FULL_story_comment_score.id, FULL_story_comment_score.score FROM FULL_story_comment_score);

CREATE VIEW FULL_story_hotness AS
SELECT all_hotness_components.id, SUM(all_hotness_components.score) as hotness
FROM all_hotness_components GROUP BY all_hotness_components.id;

-- Frontpage
CREATE VIEW frontpage_ids AS
SELECT FULL_story_hotness.id
FROM FULL_story_hotness
ORDER BY FULL_story_hotness.hotness
LIMIT 51 OFFSET 0;

-- Accessor views
CREATE VIEW story_with_hotness AS
SELECT stories.*, FULL_story_hotness.hotness
FROM stories
LEFT JOIN FULL_story_hotness ON (stories.id = FULL_story_hotness.id);

-- Other derived stats
CREATE VIEW story_comments AS
SELECT stories.id, COUNT(comments.id) as comments
FROM stories
LEFT JOIN comments ON (stories.id = comments.story_id)
GROUP BY stories.id;

CREATE VIEW user_comments AS
SELECT comments.user_id AS id, COUNT(comments.id) AS comments
FROM comments GROUP BY comments.user_id;

CREATE VIEW user_stories AS
SELECT stories.user_id AS id, COUNT(stories.id) AS stories
FROM stories GROUP BY stories.user_id;

CREATE VIEW user_stats AS
SELECT users.id, user_comments.comments, user_stories.stories
  FROM users
LEFT JOIN user_comments ON (users.id = user_comments.id)
LEFT JOIN user_stories ON (users.id = user_stories.id);

CREATE VIEW user_comment_karma AS
SELECT comment_with_votes.user_id AS id, SUM(comment_with_votes.score) AS karma
FROM comment_with_votes GROUP BY comment_with_votes.user_id;

CREATE VIEW user_story_karma AS
SELECT story_with_votes.user_id AS id, SUM(story_with_votes.score) AS karma
FROM story_with_votes GROUP BY story_with_votes.user_id;

CREATE VIEW user_karma AS
SELECT users.id, user_comment_karma.karma + user_story_karma.karma AS karma
FROM users
LEFT JOIN user_comment_karma ON (users.id = user_comment_karma.id)
LEFT JOIN user_story_karma ON (users.id = user_story_karma.id);

-----------------------------------------------------
-- Original:
-- CREATE VIEW `replying_comments` AS       select `read_ribbons`.`user_id` AS `user_id`,`comments`.`id` AS `comment_id`,`read_ribbons`.`story_id` AS `story_id`,`comments`.`parent_comment_id` AS `parent_comment_id`,`comments`.`created_at` AS `comment_created_at`,`parent_comments`.`user_id` AS `parent_comment_author_id`,`comments`.`user_id` AS `comment_author_id`,`stories`.`user_id` AS `story_author_id`,(`read_ribbons`.`updated_at` < `comments`.`created_at`) AS `is_unread`,(select `votes`.`vote` from `votes` where ((`votes`.`user_id` = `read_ribbons`.`user_id`) and (`votes`.`comment_id` = `comments`.`id`))) AS `current_vote_vote`,(select `votes`.`reason` from `votes` where ((`votes`.`user_id` = `read_ribbons`.`user_id`) and (`votes`.`comment_id` = `comments`.`id`))) AS `current_vote_reason` from (((`read_ribbons` join `comments` on((`comments`.`story_id` = `read_ribbons`.`story_id`))) join `stories` on((`stories`.`id` = `comments`.`story_id`))) left join `comments` `parent_comments` on((`parent_comments`.`id` = `comments`.`parent_comment_id`))) where ((`read_ribbons`.`is_following` = 1) and (`comments`.`user_id` <> `read_ribbons`.`user_id`) and (`comments`.`is_deleted` = 0) and (`comments`.`is_moderated` = 0) and ((`parent_comments`.`user_id` = `read_ribbons`.`user_id`) or (isnull(`parent_comments`.`user_id`) and (`stories`.`user_id` = `read_ribbons`.`user_id`))) and ((`comments`.`upvotes` - `comments`.`downvotes`) >= 0) and (isnull(`parent_comments`.`id`) or ((`parent_comments`.`upvotes` - `parent_comments`.`downvotes`) >= 0)));

CREATE VIEW `good_comments` AS
SELECT comments.id, comments.created_at, comments.story_id,
       comments.user_id, comments.parent_comment_id,
       FULL_comment_upvotes.votes - FULL_comment_downvotes.votes AS score,
FROM comments
LEFT JOIN FULL_comment_upvotes ON (comments.id = FULL_comment_upvotes.id)
LEFT JOIN FULL_comment_downvotes ON (comments.id = FULL_comment_downvotes.id)
WHERE comments.is_deleted = 0 AND comments.is_moderated = 0;

CREATE VIEW heads AS
(SELECT stories.user_id, stories.id AS story_id, stories.always_null as pid FROM stories)
UNION
(SELECT good_comments.user_id, good_comments.story_id, good_comments.id AS pid
FROM good_comments WHERE good_comments.score >= 0);

CREATE VIEW tails AS
SELECT heads.user_id, heads.story_id, good_comments.created_at
FROM heads JOIN good_comments ON (good_comments.story_id = heads.story_id)
WHERE heads.pid = good_comments.parent_comment_id;

CREATE VIEW `BOUNDARY_replying_comments_for_count` AS
SELECT `read_ribbons`.`user_id`, tails.created_at
FROM `read_ribbons`
JOIN `tails` ON (`tails`.`story_id` = `read_ribbons`.`story_id`)
WHERE `read_ribbons`.`is_following` = 1 AND
      `tails`.`user_id` <> `read_ribbons`.`user_id` AND
      `tails`.`created_at` > `read_ribbons`.`updated_at`;

CREATE VIEW BOUNDARY_notifications AS
SELECT BOUNDARY_replying_comments_for_count.user_id, COUNT(*) AS notifications,
FROM `BOUNDARY_replying_comments_for_count`
GROUP BY `BOUNDARY_replying_comments_for_count`.`user_id`;

-----------------------------------------------------

INSERT INTO `tags` (`tag`, `hotness_mod`) VALUES ('test', 1);
