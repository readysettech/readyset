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
CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS `suggested_taggings` CASCADE;
CREATE TABLE `suggested_taggings` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int, `tag_id` int, `user_id` int) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `suggested_titles` CASCADE;
CREATE TABLE `suggested_titles` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int, `user_id` int, `title` varchar(150) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `tag_filters` CASCADE;
CREATE TABLE `tag_filters` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` int, `tag_id` int,  INDEX `user_tag_idx`  (`user_id`, `tag_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `taggings` CASCADE;
CREATE TABLE `taggings` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int unsigned NOT NULL, `tag_id` int unsigned NOT NULL, UNIQUE INDEX `story_id_tag_id`  (`story_id`, `tag_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `tags` CASCADE;
CREATE TABLE `tags` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `tag` varchar(25) DEFAULT '' NOT NULL, `description` varchar(100), `privileged` tinyint(1) DEFAULT 0, `is_media` tinyint(1) DEFAULT 0, `inactive` tinyint(1) DEFAULT 0, `hotness_mod` float(24) DEFAULT 0.0, UNIQUE INDEX `tag`  (`tag`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `users` CASCADE;
CREATE TABLE `users` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `email` varchar(100) COLLATE utf8mb4_general_ci, `password_digest` varchar(75) COLLATE utf8mb4_general_ci, `created_at` datetime, `is_admin` tinyint(1) DEFAULT 0, `password_reset_token` varchar(75) COLLATE utf8mb4_general_ci, `session_token` varchar(75) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL, `about` mediumtext COLLATE utf8mb4_general_ci, `invited_by_user_id` int, `is_moderator` tinyint(1) DEFAULT 0, `pushover_mentions` tinyint(1) DEFAULT 0, `rss_token` varchar(75) COLLATE utf8mb4_general_ci, `mailing_list_token` varchar(75) COLLATE utf8mb4_general_ci, `mailing_list_mode` int DEFAULT 0, `karma` int DEFAULT 0 NOT NULL, `banned_at` datetime, `banned_by_user_id` int, `banned_reason` varchar(200) COLLATE utf8mb4_general_ci, `deleted_at` datetime, `disabled_invite_at` datetime, `disabled_invite_by_user_id` int, `disabled_invite_reason` varchar(200), `settings` text,  INDEX `mailing_list_enabled`  (`mailing_list_mode`), UNIQUE INDEX `mailing_list_token`  (`mailing_list_token`), UNIQUE INDEX `password_reset_token`  (`password_reset_token`), UNIQUE INDEX `rss_token`  (`rss_token`), UNIQUE INDEX `session_hash`  (`session_token`), UNIQUE INDEX `username`  (`username`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
DROP TABLE IF EXISTS `votes` CASCADE;
CREATE TABLE `votes` (`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int unsigned NOT NULL, `story_id` int unsigned NOT NULL, `comment_id` int unsigned, `vote` tinyint NOT NULL, `reason` varchar(1),  INDEX `index_votes_on_comment_id`  (`comment_id`),  INDEX `user_id_comment_id`  (`user_id`, `comment_id`),  INDEX `user_id_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-----------------------------------------------------
-- Make views for all the computed columns
CREATE VIEW `story_tag_hotness` AS
SELECT stories.id, SUM(tags.hotness_mod) AS hotness FROM stories
 JOIN taggings ON (taggings.story_id = stories.id)
 JOIN tags ON (tags.id = taggings.tag_id)
GROUP BY stories.id;

CREATE VIEW comment_upvotes AS SELECT votes.comment_id, votes.user_id FROM votes WHERE votes.story_id IS NULL AND votes.vote = 1;
CREATE VIEW comment_downvotes AS SELECT votes.comment_id, votes.user_id FROM votes WHERE votes.story_id IS NULL AND votes.vote = 0;
CREATE VIEW story_upvotes AS SELECT votes.story_id, votes.user_id FROM votes WHERE votes.comment_id IS NULL AND votes.vote = 1;
CREATE VIEW story_downvotes AS SELECT votes.story_id, votes.user_id FROM votes WHERE votes.comment_id IS NULL AND votes.vote = 0;

-- CREATE VIEW `comment_with_votes` AS
-- SELECT comments.*,
--        upvotes.votes AS upvotes, downvotes.votes AS downvotes,
--        upvotes.votes - downvotes.votes AS score
--  FROM comments
-- LEFT JOIN (
-- 	SELECT comment_upvotes.comment_id, COUNT(*) as votes
-- 	FROM comment_upvotes
-- 	GROUP BY comment_upvotes.comment_id
-- ) AS upvotes ON (comments.id = upvotes.comment_id)
-- LEFT JOIN (
-- 	SELECT comment_downvotes.comment_id, COUNT(*) as votes
-- 	FROM comment_downvotes
-- 	GROUP BY comment_downvotes.comment_id
-- ) AS downvotes ON (comments.id = downvotes.comment_id);
CREATE VIEW `comment_with_votes` AS SELECT comments.*, upvotes.votes AS upvotes, downvotes.votes AS downvotes, upvotes.votes - downvotes.votes AS score FROM comments LEFT JOIN (SELECT comment_upvotes.comment_id, COUNT(*) as votes FROM comment_upvotes GROUP BY comment_upvotes.comment_id) AS upvotes ON (comments.id = upvotes.comment_id) LEFT JOIN (SELECT comment_downvotes.comment_id, COUNT(*) as votes FROM comment_downvotes GROUP BY comment_downvotes.comment_id) AS downvotes ON (comments.id = downvotes.comment_id);

-- CREATE VIEW `story_with_votes` AS
-- SELECT stories.*,
--        upvotes.votes AS upvotes, downvotes.votes AS downvotes,
--        upvotes.votes - downvotes.votes AS score
--  FROM stories
-- LEFT JOIN (
-- 	SELECT story_upvotes.story_id, COUNT(*) as votes
-- 	FROM story_upvotes
-- 	GROUP BY story_upvotes.story_id
-- ) AS upvotes ON (stories.id = upvotes.story_id)
-- LEFT JOIN (
-- 	SELECT story_downvotes.story_id, COUNT(*) as votes
-- 	FROM story_downvotes
-- 	GROUP BY story_downvotes.story_id
-- ) AS downvotes ON (stories.id = downvotes.story_id);
CREATE VIEW `story_with_votes` AS SELECT stories.*, upvotes.votes AS upvotes, downvotes.votes AS downvotes, upvotes.votes - downvotes.votes AS score FROM stories LEFT JOIN (SELECT story_upvotes.story_id, COUNT(*) as votes FROM story_upvotes GROUP BY story_upvotes.story_id) AS upvotes ON (stories.id = upvotes.story_id) LEFT JOIN (SELECT story_downvotes.story_id, COUNT(*) as votes FROM story_downvotes GROUP BY story_downvotes.story_id) AS downvotes ON (stories.id = downvotes.story_id);

CREATE VIEW non_author_comment_with_votes AS
SELECT comment_with_votes.story_id,
       comment_with_votes.score
  FROM comment_with_votes
  JOIN stories ON (comment_with_votes.story_id = stories.id)
 WHERE comment_with_votes.user_id <> stories.user_id;

CREATE VIEW story_comment_score AS
SELECT non_author_comment_with_votes.story_id AS id,
       SUM(non_author_comment_with_votes.score) AS score
  FROM non_author_comment_with_votes
GROUP BY non_author_comment_with_votes.story_id;

CREATE VIEW combined_story_score AS
SELECT story_with_votes.merged_story_id AS id, SUM(story_with_votes.score) AS score
  FROM story_with_votes
 WHERE story_with_votes.merged_story_id IS NOT NULL
GROUP BY story_with_votes.merged_story_id;

CREATE VIEW story_comments AS
SELECT stories.id, COUNT(comments.id)
FROM stories
LEFT JOIN comments ON (stories.id = comments.story_id)
GROUP BY stories.id;

CREATE VIEW story_hotness_parts AS
SELECT story_with_votes.*,
       combined_story_score.score + story_comment_score.score AS cscore,
       story_tag_hotness.hotness AS tscore
FROM story_with_votes
LEFT JOIN combined_story_score ON (combined_story_score.id = story_with_votes.id)
LEFT JOIN story_comment_score ON (story_comment_score.id = story_with_votes.id)
LEFT JOIN story_tag_hotness ON (story_tag_hotness.id = story_with_votes.id);

CREATE VIEW story_hotness_part1 AS
SELECT story_hotness_parts.*, story_hotness_parts.cscore + story_hotness_parts.tscore AS extra
FROM story_hotness_parts;

CREATE VIEW story_with_hotness AS
SELECT story_hotness_part1.*, story_hotness_part1.score + story_hotness_part1.extra AS hotness
FROM story_hotness_part1;

CREATE VIEW user_comments AS
SELECT comments.user_id AS id, COUNT(comments.id) AS comments
FROM comments GROUP BY comments.user_id;

CREATE VIEW user_stories AS
SELECT stories.user_id AS id, COUNT(stories.id) AS stories
FROM stories GROUP BY stories.user_id;

CREATE VIEW user_stats AS
SELECT users.id, user_comments.comments, user_stories.stories
  FROM users
LEFT JOIN user_comments ON (user_comments.id = users.id)
LEFT JOIN user_stories ON (user_stories.id = users.id);

CREATE VIEW user_comment_karma AS
SELECT comment_with_votes.user_id AS id, SUM(comment_with_votes.score) AS karma
FROM comment_with_votes GROUP BY comment_with_votes.user_id;

CREATE VIEW user_story_karma AS
SELECT story_with_votes.user_id AS id, SUM(story_with_votes.score) AS karma
FROM story_with_votes GROUP BY story_with_votes.user_id;

CREATE VIEW user_karma AS
SELECT users.id, user_comment_karma.karma + user_story_karma.karma AS karma
FROM users
LEFT JOIN user_comment_karma ON (user_comment_karma.id = users.id)
LEFT JOIN user_story_karma ON (user_story_karma.id = users.id);
-----------------------------------------------------
-- Original:
-- CREATE VIEW `replying_comments` AS       select `read_ribbons`.`user_id` AS `user_id`,`comments`.`id` AS `comment_id`,`read_ribbons`.`story_id` AS `story_id`,`comments`.`parent_comment_id` AS `parent_comment_id`,`comments`.`created_at` AS `comment_created_at`,`parent_comments`.`user_id` AS `parent_comment_author_id`,`comments`.`user_id` AS `comment_author_id`,`stories`.`user_id` AS `story_author_id`,(`read_ribbons`.`updated_at` < `comments`.`created_at`) AS `is_unread`,(select `votes`.`vote` from `votes` where ((`votes`.`user_id` = `read_ribbons`.`user_id`) and (`votes`.`comment_id` = `comments`.`id`))) AS `current_vote_vote`,(select `votes`.`reason` from `votes` where ((`votes`.`user_id` = `read_ribbons`.`user_id`) and (`votes`.`comment_id` = `comments`.`id`))) AS `current_vote_reason` from (((`read_ribbons` join `comments` on((`comments`.`story_id` = `read_ribbons`.`story_id`))) join `stories` on((`stories`.`id` = `comments`.`story_id`))) left join `comments` `parent_comments` on((`parent_comments`.`id` = `comments`.`parent_comment_id`))) where ((`read_ribbons`.`is_following` = 1) and (`comments`.`user_id` <> `read_ribbons`.`user_id`) and (`comments`.`is_deleted` = 0) and (`comments`.`is_moderated` = 0) and ((`parent_comments`.`user_id` = `read_ribbons`.`user_id`) or (isnull(`parent_comments`.`user_id`) and (`stories`.`user_id` = `read_ribbons`.`user_id`))) and ((`comments`.`upvotes` - `comments`.`downvotes`) >= 0) and (isnull(`parent_comments`.`id`) or ((`parent_comments`.`upvotes` - `parent_comments`.`downvotes`) >= 0)));
--
-- Modified:
-- CREATE VIEW `replying_comments_for_count` AS
-- 	SELECT `read_ribbons`.`user_id`,
-- 	       `read_ribbons`.`story_id`,
-- 	       `comment_with_votes`.`id`,
-- 	FROM `read_ribbons`
-- 	JOIN `stories` ON (`stories`.`id` = `read_ribbons`.`story_id`)
-- 	JOIN `comment_with_votes` ON (`comment_with_votes`.`story_id` = `read_ribbons`.`story_id`)
-- 	LEFT JOIN `parent_comments`
-- 	ON (`parent_comments`.`id` = `comment_with_votes`.`parent_comment_id`)
-- 	WHERE `read_ribbons`.`is_following` = 1
-- 	AND `comment_with_votes`.`user_id` <> `read_ribbons`.`user_id`
-- 	AND `comment_with_votes`.`is_deleted` = 0
-- 	AND `comment_with_votes`.`is_moderated` = 0
-- 	AND `comment_with_votes`.`score` >= 0
-- 	AND `read_ribbons`.`updated_at` < `comment_with_votes`.`created_at`
-- 	AND (
--      (
--      	`parent_comments`.`user_id` = `read_ribbons`.`user_id`
--      	AND
--      	`parent_`.`score` >= 0
--      )
--      OR
--      (
--      	`parent_comments`.`id` IS NULL
--      	AND
--      	`stories`.`user_id` = `read_ribbons`.`user_id`
--      )
--      );

--
-- Without newlines:
CREATE VIEW `parent_comments` AS SELECT `comment_with_votes`.* FROM `comment_with_votes`;
CREATE VIEW `replying_comments_for_count` AS SELECT `read_ribbons`.`user_id`, `read_ribbons`.`story_id`, `comment_with_votes`.`id`, FROM `read_ribbons` JOIN `stories` ON (`stories`.`id` = `read_ribbons`.`story_id`) JOIN `comment_with_votes` ON (`comment_with_votes`.`story_id` = `read_ribbons`.`story_id`) LEFT JOIN `parent_comments` ON (`parent_comments`.`id` = `comment_with_votes`.`parent_comment_id`) WHERE `read_ribbons`.`is_following` = 1 AND `comment_with_votes`.`user_id` <> `read_ribbons`.`user_id` AND `comment_with_votes`.`is_deleted` = 0 AND `comment_with_votes`.`is_moderated` = 0 AND `comment_with_votes`.`score` >= 0 AND `read_ribbons`.`updated_at` < `comment_with_votes`.`created_at` AND ( ( `parent_comments`.`user_id` = `read_ribbons`.`user_id` AND `parent_`.`score` >= 0) OR ( `parent_comments`.`id` IS NULL AND `stories`.`user_id` = `read_ribbons`.`user_id`));
-----------------------------------------------------
INSERT INTO `tags` (`tag`) VALUES ('test');
