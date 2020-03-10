-- main.rs
CREATE VIEW login_1 AS SELECT 1 as one FROM `users` WHERE `users`.`username` = ?;

-- endpoints/mod.rs
CREATE VIEW notif_1 AS SELECT BOUNDARY_notifications.notifications FROM BOUNDARY_notifications WHERE BOUNDARY_notifications.user_id = ?;
CREATE VIEW notif_2 AS SELECT `keystores`.* FROM `keystores` WHERE `keystores`.`key` = ?;

-- endpoints/comment.rs
CREATE VIEW comment_1 AS SELECT `stories`.* FROM `stories` WHERE `stories`.`short_id` = ?;
CREATE VIEW comment_2 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
CREATE VIEW comment_3 AS SELECT  `comments`.* FROM `comments` WHERE `comments`.`story_id` = ? AND `comments`.`short_id` = ?;
CREATE VIEW comment_4 AS SELECT  1 AS one FROM `comments` WHERE `comments`.`short_id` = ?;
CREATE VIEW comment_5 AS SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` = ?;

-- endpoints/comment_vote.rs
CREATE VIEW comment_vote_1 AS SELECT `comments`.* FROM `comments` WHERE `comments`.`short_id` = ?;
CREATE VIEW comment_vote_2 AS SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` = ?;

-- endpoints/comments.rs
CREATE VIEW comments_1 AS SELECT  `comments`.id FROM `comments` WHERE `comments`.`is_deleted` = 0 AND `comments`.`is_moderated` = 0 ORDER BY id DESC LIMIT 40 OFFSET 0;
CREATE VIEW comments_2 AS SELECT `comment_with_votes`.id, comment_with_votes.user_id, comment_with_votes.story_id FROM `comment_with_votes` WHERE comment_with_votes.id = ?;
CREATE VIEW comments_3 AS SELECT 1 FROM hidden_stories WHERE user_id = ? AND hidden_stories.story_id = ?;
CREATE VIEW comments_4 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
CREATE VIEW comments_5 AS SELECT  `story_with_votes`.user_id FROM `story_with_votes` WHERE `story_with_votes`.`id` = ?;
CREATE VIEW comments_6 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`comment_id` = ?;
CREATE VIEW comments_7 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;

-- endpoints/frontpage.rs
CREATE VIEW frontpage_1 AS SELECT `frontpage_ids`.id FROM `frontpage_ids`;
CREATE VIEW frontpage_2 AS SELECT `story_with_votes`.user_id FROM `story_with_votes` WHERE `story_with_votes`.`id` = ? AND `story_with_votes`.`merged_story_id` IS NULL AND `story_with_votes`.`is_expired` = 0 AND `story_with_votes`.`score` >= 0;
CREATE VIEW frontpage_3 AS SELECT `tag_filters`.* FROM `tag_filters` WHERE `tag_filters`.`user_id` = ?;
CREATE VIEW frontpage_4 AS SELECT `taggings`.`story_id` FROM `taggings` WHERE `taggings`.`story_id` = ?;
CREATE VIEW frontpage_5 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
CREATE VIEW frontpage_6 AS SELECT `suggested_titles`.* FROM `suggested_titles` WHERE `suggested_titles`.`story_id` = ?;
CREATE VIEW frontpage_7 AS SELECT `suggested_taggings`.* FROM `suggested_taggings` WHERE `suggested_taggings`.`story_id` = ?;
CREATE VIEW frontpage_8 AS SELECT `taggings`.tag_id FROM `taggings` WHERE `taggings`.`story_id` = ?;
CREATE VIEW frontpage_9 AS SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;
CREATE VIEW frontpage_10 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
CREATE VIEW frontpage_11 AS SELECT `hidden_stories`.* FROM `hidden_stories` WHERE `hidden_stories`.`user_id` = ? AND `hidden_stories`.`story_id` = ?;
CREATE VIEW frontpage_12 AS SELECT `saved_stories`.* FROM `saved_stories` WHERE `saved_stories`.`user_id` = ? AND `saved_stories`.`story_id` = ?;

-- endpoints/recent.rs
CREATE VIEW recent_1 AS SELECT `stories`.id FROM `stories` WHERE `stories`.`merged_story_id` IS NULL AND `stories`.`is_expired` = 0 ORDER BY stories.id DESC LIMIT 51;
CREATE VIEW recent_2 AS SELECT `story_with_votes`.user_id FROM `story_with_votes` WHERE `story_with_votes`.`id` = ?;
CREATE VIEW recent_3 AS SELECT `tag_filters`.* FROM `tag_filters` WHERE `tag_filters`.`user_id` = ?;
CREATE VIEW recent_4 AS SELECT `taggings`.`story_id` FROM `taggings` WHERE `taggings`.`story_id` = ?;
CREATE VIEW recent_5 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
CREATE VIEW recent_6 AS SELECT `suggested_titles`.* FROM `suggested_titles` WHERE `suggested_titles`.`story_id` = ?;
CREATE VIEW recent_7 AS SELECT `suggested_taggings`.* FROM `suggested_taggings` WHERE `suggested_taggings`.`story_id` = ?;
CREATE VIEW recent_8 AS SELECT `taggings`.tag_id FROM `taggings` WHERE `taggings`.`story_id` = ?;
CREATE VIEW recent_9 AS SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;
CREATE VIEW recent_10 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
CREATE VIEW recent_11 AS SELECT `hidden_stories`.* FROM `hidden_stories` WHERE `hidden_stories`.`user_id` = ? AND `hidden_stories`.`story_id` = ?;
CREATE VIEW recent_12 AS SELECT `saved_stories`.* FROM `saved_stories` WHERE `saved_stories`.`user_id` = ? AND `saved_stories`.`story_id` = ?;

-- endpoints/story.rs
CREATE VIEW story_1 AS SELECT `story_with_votes`.* FROM `story_with_votes` WHERE `story_with_votes`.`short_id` = ?;
CREATE VIEW story_2 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
CREATE VIEW story_3 AS SELECT `read_ribbons`.* FROM `read_ribbons` WHERE `read_ribbons`.`user_id` = ? AND `read_ribbons`.`story_id` = ?;
CREATE VIEW story_4 AS SELECT `stories`.`id` FROM `stories` WHERE `stories`.`merged_story_id` = ?;
CREATE VIEW story_5 AS SELECT `comment_with_votes`.* FROM `comment_with_votes` WHERE `comment_with_votes`.`story_id` = ? ORDER BY comment_with_votes.score DESC;
CREATE VIEW story_6 AS SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
CREATE VIEW story_7 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`comment_id` = ?;
CREATE VIEW story_8 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
CREATE VIEW story_9 AS SELECT `hidden_stories`.* FROM `hidden_stories` WHERE `hidden_stories`.`user_id` = ? AND `hidden_stories`.`story_id` = ?;
CREATE VIEW story_10 AS SELECT `saved_stories`.* FROM `saved_stories` WHERE `saved_stories`.`user_id` = ? AND `saved_stories`.`story_id` = ?;
CREATE VIEW story_11 AS SELECT `taggings`.tag_id FROM `taggings` WHERE `taggings`.`story_id` = ?;
CREATE VIEW story_12 AS SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;

-- endpoints/user.rs
CREATE VIEW user_1 AS SELECT `users`.* FROM `users` WHERE `users`.`username` = ?;
CREATE VIEW user_2 AS SELECT user_stats.* FROM user_stats WHERE user_stats.id = ?;
CREATE VIEW user_3 AS SELECT `tags`.`id`, COUNT(*) AS `count` FROM `taggings` INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` WHERE `tags`.`inactive` = 0 AND `stories`.`user_id` = ? GROUP BY `tags`.`id` ORDER BY `count` desc LIMIT 1;
CREATE VIEW user_4 AS SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;
CREATE VIEW user_5 AS SELECT 1 AS one FROM `hats` WHERE `hats`.`user_id` = ? LIMIT 1;

-- endpoints/story_vote.rs
CREATE VIEW story_vote_1 AS SELECT `stories`.* FROM `stories` WHERE `stories`.`short_id` = ?;
CREATE VIEW story_vote_2 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;

-- endpoints/submit.rs
CREATE VIEW submit_1 AS SELECT `tags`.* FROM `tags` WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test');
CREATE VIEW submit_2 AS SELECT 1 AS one FROM `stories` WHERE `stories`.`short_id` = ?;
CREATE VIEW submit_3 AS SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
