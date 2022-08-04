CREATE TABLE `comments` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime, `short_id` varchar(10) DEFAULT '' NOT NULL, `story_id` int unsigned NOT NULL, `user_id` int unsigned NOT NULL, `parent_comment_id` int unsigned, `thread_id` int unsigned, `comment` mediumtext NOT NULL, `markeddown_comment` mediumtext, `is_deleted` tinyint(1) DEFAULT 0, `is_moderated` tinyint(1) DEFAULT 0, `is_from_email` tinyint(1) DEFAULT 0, `hat_id` int, fulltext INDEX `index_comments_on_comment`  (`comment`),  INDEX `confidence_idx`  (`confidence`), UNIQUE INDEX `short_id`  (`short_id`),  INDEX `story_id_short_id`  (`story_id`, `short_id`),  INDEX `thread_id`  (`thread_id`),  INDEX `index_comments_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `hat_requests` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `updated_at` datetime, `user_id` int, `hat` varchar(255) COLLATE utf8mb4_general_ci, `link` varchar(255) COLLATE utf8mb4_general_ci, `comment` text COLLATE utf8mb4_general_ci) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `hats` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `updated_at` datetime, `user_id` int, `granted_by_user_id` int, `hat` varchar(255) NOT NULL, `link` varchar(255) COLLATE utf8mb4_general_ci, `modlog_use` tinyint(1) DEFAULT 0, `doffed_at` datetime) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `hidden_stories` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int, `story_id` int, UNIQUE INDEX `index_hidden_stories_on_user_id_and_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `invitation_requests` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `code` varchar(255), `is_verified` tinyint(1) DEFAULT 0, `email` varchar(255), `name` varchar(255), `memo` text, `ip_address` varchar(255), `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `invitations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int, `email` varchar(255), `code` varchar(255), `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `memo` mediumtext) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `keystores` (`key` varchar(50) DEFAULT '' NOT NULL, `value` bigint, PRIMARY KEY (`key`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `messages` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime, `author_user_id` int unsigned, `recipient_user_id` int unsigned, `has_been_read` tinyint(1) DEFAULT 0, `subject` varchar(100), `body` mediumtext, `short_id` varchar(30), `deleted_by_author` tinyint(1) DEFAULT 0, `deleted_by_recipient` tinyint(1) DEFAULT 0, UNIQUE INDEX `random_hash`  (`short_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `moderations` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `moderator_user_id` int, `story_id` int, `comment_id` int, `user_id` int, `action` mediumtext, `reason` mediumtext, `is_from_suggestions` tinyint(1) DEFAULT 0,  INDEX `index_moderations_on_created_at`  (`created_at`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `read_ribbons` (`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, `is_following` tinyint(1) DEFAULT 1, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` bigint, `story_id` bigint,  INDEX `index_read_ribbons_on_story_id`  (`story_id`),  INDEX `index_read_ribbons_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `saved_stories` (`id` bigint NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` int, `story_id` int, UNIQUE INDEX `index_saved_stories_on_user_id_and_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- XXX: stories doesn't usually have an always-NULL column
CREATE TABLE `stories` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `always_null` int, `created_at` datetime, `user_id` int unsigned, `url` varchar(250) DEFAULT '', `title` varchar(150) DEFAULT '' NOT NULL, `description` mediumtext, `short_id` varchar(6) DEFAULT '' NOT NULL, `is_expired` tinyint(1) DEFAULT 0 NOT NULL, `is_moderated` tinyint(1) DEFAULT 0 NOT NULL, `markeddown_description` mediumtext, `story_cache` mediumtext, `merged_story_id` int, `unavailable_at` datetime, `twitter_id` varchar(20), `user_is_author` tinyint(1) DEFAULT 0,  INDEX `index_stories_on_created_at`  (`created_at`), fulltext INDEX `index_stories_on_description`  (`description`),   INDEX `is_idxes`  (`is_expired`, `is_moderated`),  INDEX `index_stories_on_is_expired`  (`is_expired`),  INDEX `index_stories_on_is_moderated`  (`is_moderated`),  INDEX `index_stories_on_merged_story_id`  (`merged_story_id`), UNIQUE INDEX `unique_short_id`  (`short_id`), fulltext INDEX `index_stories_on_story_cache`  (`story_cache`), fulltext INDEX `index_stories_on_title`  (`title`),  INDEX `index_stories_on_twitter_id`  (`twitter_id`),  INDEX `url`  (`url`(191)),  INDEX `index_stories_on_user_id`  (`user_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE `suggested_taggings` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int, `tag_id` int, `user_id` int) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `suggested_titles` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int, `user_id` int, `title` varchar(150) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `tag_filters` (`id` int NOT NULL AUTO_INCREMENT PRIMARY KEY, `created_at` datetime NOT NULL, `updated_at` datetime NOT NULL, `user_id` int, `tag_id` int,  INDEX `user_tag_idx`  (`user_id`, `tag_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `taggings` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `story_id` int unsigned NOT NULL, `tag_id` int unsigned NOT NULL, UNIQUE INDEX `story_id_tag_id`  (`story_id`, `tag_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- XXX: hotness_mod should be a float(24)
CREATE TABLE `tags` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `tag` varchar(25) DEFAULT '' NOT NULL, `description` varchar(100), `privileged` tinyint(1) DEFAULT 0, `is_media` tinyint(1) DEFAULT 0, `inactive` tinyint(1) DEFAULT 0, `hotness_mod` int DEFAULT 0, UNIQUE INDEX `tag`  (`tag`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `users` (`id` int unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `username` varchar(50) COLLATE utf8mb4_general_ci, `email` varchar(100) COLLATE utf8mb4_general_ci, `password_digest` varchar(75) COLLATE utf8mb4_general_ci, `created_at` datetime, `is_admin` tinyint(1) DEFAULT 0, `password_reset_token` varchar(75) COLLATE utf8mb4_general_ci, `session_token` varchar(75) COLLATE utf8mb4_general_ci DEFAULT '' NOT NULL, `about` mediumtext COLLATE utf8mb4_general_ci, `invited_by_user_id` int, `is_moderator` tinyint(1) DEFAULT 0, `pushover_mentions` tinyint(1) DEFAULT 0, `rss_token` varchar(75) COLLATE utf8mb4_general_ci, `mailing_list_token` varchar(75) COLLATE utf8mb4_general_ci, `mailing_list_mode` int DEFAULT 0, `karma` int DEFAULT 0 NOT NULL, `banned_at` datetime, `banned_by_user_id` int, `banned_reason` varchar(200) COLLATE utf8mb4_general_ci, `deleted_at` datetime, `disabled_invite_at` datetime, `disabled_invite_by_user_id` int, `disabled_invite_reason` varchar(200), `settings` text,  INDEX `mailing_list_enabled`  (`mailing_list_mode`), UNIQUE INDEX `mailing_list_token`  (`mailing_list_token`), UNIQUE INDEX `password_reset_token`  (`password_reset_token`), UNIQUE INDEX `rss_token`  (`rss_token`), UNIQUE INDEX `session_hash`  (`session_token`), UNIQUE INDEX `username`  (`username`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;
CREATE TABLE `votes` (`id` bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `user_id` int unsigned NOT NULL, `story_id` int unsigned NOT NULL, `comment_id` int unsigned, `vote` tinyint NOT NULL, `reason` varchar(1),  INDEX `index_votes_on_comment_id`  (`comment_id`),  INDEX `user_id_comment_id`  (`user_id`, `comment_id`),  INDEX `user_id_story_id`  (`user_id`, `story_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-----------------------------------------------------
-- Make views for all the computed columns
CREATE VIEW `FULL_story_tag_score` AS SELECT taggings.story_id AS id, SUM(tags.hotness_mod) AS score FROM taggings JOIN tags ON (tags.id = taggings.tag_id) GROUP BY taggings.story_id;

-- Comment score tracking

CREATE VIEW FULL_comment_upvotes AS SELECT votes.comment_id AS id, COUNT(*) as votes FROM votes WHERE votes.story_id IS NULL AND votes.vote = 1 GROUP BY votes.comment_id;

CREATE VIEW FULL_comment_downvotes AS SELECT votes.comment_id AS id, COUNT(*) as votes FROM votes WHERE votes.story_id IS NULL AND votes.vote = 0 GROUP BY votes.comment_id;

CREATE VIEW comment_votes AS (SELECT FULL_comment_upvotes.id, FULL_comment_upvotes.votes AS score FROM FULL_comment_upvotes) UNION (SELECT FULL_comment_downvotes.id, 0 - FULL_comment_downvotes.votes AS score FROM FULL_comment_downvotes);

CREATE VIEW FULL_comment_score AS SELECT comment_votes.id, SUM(comment_votes.score) as score FROM comment_votes GROUP BY comment_votes.id;

-- Story score tracking

CREATE VIEW FULL_story_upvotes AS SELECT votes.story_id AS id, COUNT(*) as votes FROM votes WHERE votes.comment_id IS NULL AND votes.vote = 1 GROUP BY votes.story_id;

CREATE VIEW FULL_story_downvotes AS SELECT votes.story_id AS id, COUNT(*) as votes FROM votes WHERE votes.comment_id IS NULL AND votes.vote = 0 GROUP BY votes.story_id;

CREATE VIEW story_votes AS (SELECT FULL_story_upvotes.id, FULL_story_upvotes.votes AS score FROM FULL_story_upvotes) UNION (SELECT FULL_story_downvotes.id, 0 - FULL_story_downvotes.votes AS score FROM FULL_story_downvotes);

CREATE VIEW FULL_story_score AS SELECT story_votes.id, SUM(story_votes.score) as score FROM story_votes GROUP BY story_votes.id;

-- Useful intermediate views
CREATE VIEW `comment_with_votes` AS SELECT comments.*, FULL_comment_upvotes.votes AS upvotes, FULL_comment_downvotes.votes AS downvotes, FULL_comment_upvotes.votes - FULL_comment_downvotes.votes AS score FROM comments LEFT JOIN FULL_comment_upvotes ON (comments.id = FULL_comment_upvotes.id) LEFT JOIN FULL_comment_downvotes ON (comments.id = FULL_comment_downvotes.id);

CREATE VIEW `story_with_votes` AS SELECT stories.*, FULL_story_upvotes.votes AS upvotes, FULL_story_downvotes.votes AS downvotes, FULL_story_upvotes.votes - FULL_story_downvotes.votes AS score FROM stories LEFT JOIN FULL_story_upvotes ON (stories.id = FULL_story_upvotes.id) LEFT JOIN FULL_story_downvotes ON (stories.id = FULL_story_downvotes.id);

-- Hotness computation
-- XXX: bah.. pretty sad that this join will end up full...
CREATE VIEW FULL_non_author_comments AS SELECT comments.id, comments.story_id FROM comments JOIN stories ON (comments.story_id = stories.id) WHERE comments.user_id <> stories.user_id;

CREATE VIEW FULL_story_comment_score AS SELECT FULL_non_author_comments.story_id AS id, SUM(FULL_comment_score.score) AS score FROM FULL_non_author_comments JOIN FULL_comment_score ON (FULL_comment_score.id = FULL_non_author_comments.id) GROUP BY FULL_non_author_comments.story_id;

CREATE VIEW FULL_merged_story_score AS SELECT stories.merged_story_id AS id, FULL_story_score.score FROM FULL_story_score JOIN stories ON (FULL_story_score.id = stories.merged_story_id);

-- XXX: *technically* tag_score should be a multiplier
CREATE VIEW all_hotness_components AS (SELECT FULL_story_tag_score.id, FULL_story_tag_score.score FROM FULL_story_tag_score) UNION (SELECT FULL_story_score.id, FULL_story_score.score FROM FULL_story_score) UNION (SELECT FULL_merged_story_score.id, FULL_merged_story_score.score FROM FULL_merged_story_score) UNION (SELECT FULL_story_comment_score.id, FULL_story_comment_score.score FROM FULL_story_comment_score);

CREATE VIEW FULL_story_hotness AS SELECT all_hotness_components.id, SUM(all_hotness_components.score) as hotness FROM all_hotness_components GROUP BY all_hotness_components.id;

-- Frontpage
CREATE VIEW frontpage_ids AS SELECT FULL_story_hotness.id FROM FULL_story_hotness ORDER BY FULL_story_hotness.hotness LIMIT 51 OFFSET 0;

-- Accessor views
CREATE VIEW story_with_hotness AS SELECT stories.*, FULL_story_hotness.hotness FROM stories LEFT JOIN FULL_story_hotness ON (stories.id = FULL_story_hotness.id);

-- Other derived stats
CREATE VIEW story_comments AS SELECT stories.id, COUNT(comments.id) as comments FROM stories LEFT JOIN comments ON (stories.id = comments.story_id) GROUP BY stories.id;

CREATE VIEW user_comments AS SELECT comments.user_id AS id, COUNT(comments.id) AS comments FROM comments GROUP BY comments.user_id;

CREATE VIEW user_stories AS SELECT stories.user_id AS id, COUNT(stories.id) AS stories FROM stories GROUP BY stories.user_id;

CREATE VIEW user_stats AS SELECT users.id, user_comments.comments, user_stories.stories FROM users LEFT JOIN user_comments ON (users.id = user_comments.id) LEFT JOIN user_stories ON (users.id = user_stories.id);

CREATE VIEW user_comment_karma AS SELECT comment_with_votes.user_id AS id, SUM(comment_with_votes.score) AS karma FROM comment_with_votes GROUP BY comment_with_votes.user_id;

CREATE VIEW user_story_karma AS SELECT story_with_votes.user_id AS id, SUM(story_with_votes.score) AS karma FROM story_with_votes GROUP BY story_with_votes.user_id;

CREATE VIEW user_karma AS SELECT users.id, user_comment_karma.karma + user_story_karma.karma AS karma FROM users LEFT JOIN user_comment_karma ON (users.id = user_comment_karma.id) LEFT JOIN user_story_karma ON (users.id = user_story_karma.id);

-----------------------------------------------------
-- Original:
-- CREATE VIEW `replying_comments` AS       select `read_ribbons`.`user_id` AS `user_id`,`comments`.`id` AS `comment_id`,`read_ribbons`.`story_id` AS `story_id`,`comments`.`parent_comment_id` AS `parent_comment_id`,`comments`.`created_at` AS `comment_created_at`,`parent_comments`.`user_id` AS `parent_comment_author_id`,`comments`.`user_id` AS `comment_author_id`,`stories`.`user_id` AS `story_author_id`,(`read_ribbons`.`updated_at` < `comments`.`created_at`) AS `is_unread`,(select `votes`.`vote` from `votes` where ((`votes`.`user_id` = `read_ribbons`.`user_id`) and (`votes`.`comment_id` = `comments`.`id`))) AS `current_vote_vote`,(select `votes`.`reason` from `votes` where ((`votes`.`user_id` = `read_ribbons`.`user_id`) and (`votes`.`comment_id` = `comments`.`id`))) AS `current_vote_reason` from (((`read_ribbons` join `comments` on((`comments`.`story_id` = `read_ribbons`.`story_id`))) join `stories` on((`stories`.`id` = `comments`.`story_id`))) left join `comments` `parent_comments` on((`parent_comments`.`id` = `comments`.`parent_comment_id`))) where ((`read_ribbons`.`is_following` = 1) and (`comments`.`user_id` <> `read_ribbons`.`user_id`) and (`comments`.`is_deleted` = 0) and (`comments`.`is_moderated` = 0) and ((`parent_comments`.`user_id` = `read_ribbons`.`user_id`) or (isnull(`parent_comments`.`user_id`) and (`stories`.`user_id` = `read_ribbons`.`user_id`))) and ((`comments`.`upvotes` - `comments`.`downvotes`) >= 0) and (isnull(`parent_comments`.`id`) or ((`parent_comments`.`upvotes` - `parent_comments`.`downvotes`) >= 0)));

CREATE VIEW `good_comments` AS SELECT comments.id, comments.created_at, comments.story_id, comments.user_id, comments.parent_comment_id, FULL_comment_upvotes.votes - FULL_comment_downvotes.votes AS score FROM comments LEFT JOIN FULL_comment_upvotes ON (comments.id = FULL_comment_upvotes.id) LEFT JOIN FULL_comment_downvotes ON (comments.id = FULL_comment_downvotes.id) WHERE comments.is_deleted = 0 AND comments.is_moderated = 0;

CREATE VIEW heads AS (SELECT stories.user_id, stories.id AS story_id, stories.always_null as pid FROM stories) UNION (SELECT good_comments.user_id, good_comments.story_id, good_comments.id AS pid FROM good_comments WHERE good_comments.score >= 0);

CREATE VIEW tails AS SELECT heads.user_id, heads.story_id, good_comments.created_at FROM heads JOIN good_comments ON (good_comments.story_id = heads.story_id) WHERE heads.pid = good_comments.parent_comment_id;

CREATE VIEW `BOUNDARY_replying_comments_for_count` AS SELECT `read_ribbons`.`user_id`, tails.created_at FROM `read_ribbons` JOIN `tails` ON (`tails`.`story_id` = `read_ribbons`.`story_id`) WHERE `read_ribbons`.`is_following` = 1 AND `tails`.`user_id` <> `read_ribbons`.`user_id` AND `tails`.`created_at` > `read_ribbons`.`updated_at`;

CREATE VIEW BOUNDARY_notifications AS SELECT BOUNDARY_replying_comments_for_count.user_id, COUNT(*) AS notifications, FROM `BOUNDARY_replying_comments_for_count` GROUP BY `BOUNDARY_replying_comments_for_count`.`user_id`;

-- Runtime queries

CREATE CACHE q_0 FROM SELECT 1 AS one FROM users WHERE users.username = ?;
CREATE CACHE q_1 FROM SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.inactive = 0 AND tags.tag = ?;
CREATE CACHE q_2 FROM SELECT 1 AS one FROM stories WHERE stories.short_id = ?;
CREATE CACHE q_3 FROM SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = ? AND votes.story_id = ? AND votes.comment_id = NULL;
CREATE CACHE q_4 FROM SELECT stories.id, stories.always_null, stories.created_at, stories.user_id, stories.url, stories.title, stories.description, stories.short_id, stories.is_expired, stories.is_moderated, stories.markeddown_description, stories.story_cache, stories.merged_story_id, stories.unavailable_at, stories.twitter_id, stories.user_is_author FROM stories WHERE stories.short_id = ?;
CREATE CACHE q_5 FROM SELECT users.id, users.username, users.email, users.password_digest, users.created_at, users.is_admin, users.password_reset_token, users.session_token, users.about, users.invited_by_user_id, users.is_moderator, users.pushover_mentions, users.rss_token, users.mailing_list_token, users.mailing_list_mode, users.karma, users.banned_at, users.banned_by_user_id, users.banned_reason, users.deleted_at, users.disabled_invite_at, users.disabled_invite_by_user_id, users.disabled_invite_reason, users.settings FROM users WHERE users.id = ?;
CREATE CACHE q_6 FROM SELECT 1 AS one FROM comments WHERE comments.short_id = ?;
CREATE CACHE q_7 FROM SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = ? AND votes.story_id = ? AND votes.comment_id = ?;
CREATE CACHE q_8 FROM SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.story_id = ? AND comments.short_id = ?;
CREATE VIEW q_9 AS SELECT frontpage_ids.id FROM frontpage_ids;
CREATE CACHE q_10 FROM SELECT story_with_votes.id, story_with_votes.always_null, story_with_votes.created_at, story_with_votes.user_id, story_with_votes.url, story_with_votes.title, story_with_votes.description, story_with_votes.short_id, story_with_votes.is_expired, story_with_votes.is_moderated, story_with_votes.markeddown_description, story_with_votes.story_cache, story_with_votes.merged_story_id, story_with_votes.unavailable_at, story_with_votes.twitter_id, story_with_votes.user_is_author, story_with_votes.upvotes, story_with_votes.downvotes, story_with_votes.score FROM story_with_votes WHERE story_with_votes.id = ? AND story_with_votes.merged_story_id = NULL AND story_with_votes.is_expired = 0 AND story_with_votes.score >= 0;
CREATE CACHE q_11 FROM SELECT story_with_votes.id, story_with_votes.always_null, story_with_votes.created_at, story_with_votes.user_id, story_with_votes.url, story_with_votes.title, story_with_votes.description, story_with_votes.short_id, story_with_votes.is_expired, story_with_votes.is_moderated, story_with_votes.markeddown_description, story_with_votes.story_cache, story_with_votes.merged_story_id, story_with_votes.unavailable_at, story_with_votes.twitter_id, story_with_votes.user_is_author, story_with_votes.upvotes, story_with_votes.downvotes, story_with_votes.score FROM story_with_votes WHERE story_with_votes.short_id = ?;
CREATE CACHE q_12 FROM SELECT tag_filters.id, tag_filters.created_at, tag_filters.updated_at, tag_filters.user_id, tag_filters.tag_id FROM tag_filters WHERE tag_filters.user_id = ?;
CREATE CACHE q_13 FROM SELECT read_ribbons.id, read_ribbons.is_following, read_ribbons.created_at, read_ribbons.updated_at, read_ribbons.user_id, read_ribbons.story_id FROM read_ribbons WHERE read_ribbons.user_id = ? AND read_ribbons.story_id = ?;
CREATE CACHE q_14 FROM SELECT taggings.story_id FROM taggings WHERE taggings.story_id = ?;
CREATE CACHE q_15 FROM SELECT stories.id FROM stories WHERE stories.merged_story_id = ?;
CREATE CACHE q_16 FROM SELECT suggested_titles.id, suggested_titles.story_id, suggested_titles.user_id, suggested_titles.title FROM suggested_titles WHERE suggested_titles.story_id = ?;
CREATE CACHE q_17 FROM SELECT comment_with_votes.id, comment_with_votes.created_at, comment_with_votes.updated_at, comment_with_votes.short_id, comment_with_votes.story_id, comment_with_votes.user_id, comment_with_votes.parent_comment_id, comment_with_votes.thread_id, comment_with_votes.comment, comment_with_votes.markeddown_comment, comment_with_votes.is_deleted, comment_with_votes.is_moderated, comment_with_votes.is_from_email, comment_with_votes.hat_id, comment_with_votes.upvotes, comment_with_votes.downvotes, comment_with_votes.score FROM comment_with_votes WHERE comment_with_votes.story_id = ? ORDER BY score DESC;
CREATE CACHE q_18 FROM SELECT suggested_taggings.id, suggested_taggings.story_id, suggested_taggings.tag_id, suggested_taggings.user_id FROM suggested_taggings WHERE suggested_taggings.story_id = ?;
CREATE CACHE q_19 FROM SELECT votes.id, votes.user_id, votes.story_id, votes.comment_id, votes.vote, votes.reason FROM votes WHERE votes.user_id = ? AND votes.comment_id = ?;
CREATE CACHE q_20 FROM SELECT taggings.id, taggings.story_id, taggings.tag_id FROM taggings WHERE taggings.story_id = ?;
CREATE CACHE q_21 FROM SELECT tags.id, tags.tag, tags.description, tags.privileged, tags.is_media, tags.inactive, tags.hotness_mod FROM tags WHERE tags.id = ?;
CREATE CACHE q_22 FROM SELECT hidden_stories.id, hidden_stories.user_id, hidden_stories.story_id FROM hidden_stories WHERE hidden_stories.user_id = ? AND hidden_stories.story_id = ?;
CREATE CACHE q_23 FROM SELECT users.id, users.username, users.email, users.password_digest, users.created_at, users.is_admin, users.password_reset_token, users.session_token, users.about, users.invited_by_user_id, users.is_moderator, users.pushover_mentions, users.rss_token, users.mailing_list_token, users.mailing_list_mode, users.karma, users.banned_at, users.banned_by_user_id, users.banned_reason, users.deleted_at, users.disabled_invite_at, users.disabled_invite_by_user_id, users.disabled_invite_reason, users.settings FROM users WHERE users.username = ?;
CREATE CACHE q_24 FROM SELECT saved_stories.id, saved_stories.created_at, saved_stories.updated_at, saved_stories.user_id, saved_stories.story_id FROM saved_stories WHERE saved_stories.user_id = ? AND saved_stories.story_id = ?;
CREATE CACHE q_25 FROM SELECT user_stats.id, user_stats.comments, user_stats.stories FROM user_stats WHERE user_stats.id = ?;
CREATE CACHE q_26 FROM SELECT BOUNDARY_notifications.notifications FROM BOUNDARY_notifications WHERE BOUNDARY_notifications.user_id = ?;
CREATE CACHE q_27 FROM SELECT tags.id, count(*) AS count FROM taggings INNER JOIN tags ON taggings.tag_id = tags.id INNER JOIN stories ON stories.id = taggings.story_id WHERE tags.inactive = 0 AND stories.user_id = ? GROUP BY tags.id ORDER BY count DESC LIMIT 1;
CREATE CACHE q_28 FROM SELECT keystores.`key`, keystores.value FROM keystores WHERE keystores.`key` = ?;
CREATE CACHE q_29 FROM SELECT 1 AS one FROM hats WHERE hats.user_id = ? LIMIT 1;
CREATE VIEW q_30 AS SELECT stories.id FROM stories WHERE stories.merged_story_id = NULL AND stories.is_expired = 0 ORDER BY id DESC LIMIT 51;
CREATE CACHE q_31 FROM SELECT story_with_votes.id, story_with_votes.always_null, story_with_votes.created_at, story_with_votes.user_id, story_with_votes.url, story_with_votes.title, story_with_votes.description, story_with_votes.short_id, story_with_votes.is_expired, story_with_votes.is_moderated, story_with_votes.markeddown_description, story_with_votes.story_cache, story_with_votes.merged_story_id, story_with_votes.unavailable_at, story_with_votes.twitter_id, story_with_votes.user_is_author, story_with_votes.upvotes, story_with_votes.downvotes, story_with_votes.score FROM story_with_votes WHERE story_with_votes.id = ?;
CREATE VIEW q_32 AS SELECT comments.id FROM comments WHERE comments.is_deleted = 0 AND comments.is_moderated = 0 ORDER BY id DESC LIMIT 40;
CREATE CACHE q_33 FROM SELECT comment_with_votes.id, comment_with_votes.created_at, comment_with_votes.updated_at, comment_with_votes.short_id, comment_with_votes.story_id, comment_with_votes.user_id, comment_with_votes.parent_comment_id, comment_with_votes.thread_id, comment_with_votes.comment, comment_with_votes.markeddown_comment, comment_with_votes.is_deleted, comment_with_votes.is_moderated, comment_with_votes.is_from_email, comment_with_votes.hat_id, comment_with_votes.upvotes, comment_with_votes.downvotes, comment_with_votes.score FROM comment_with_votes WHERE comment_with_votes.id = ?;
CREATE CACHE q_34 FROM SELECT 1 FROM hidden_stories WHERE user_id = ? AND hidden_stories.story_id = ?;
CREATE CACHE q_35 FROM SELECT comments.id, comments.created_at, comments.updated_at, comments.short_id, comments.story_id, comments.user_id, comments.parent_comment_id, comments.thread_id, comments.comment, comments.markeddown_comment, comments.is_deleted, comments.is_moderated, comments.is_from_email, comments.hat_id FROM comments WHERE comments.short_id = ?;
