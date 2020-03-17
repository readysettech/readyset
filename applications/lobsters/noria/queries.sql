-- main.rs
QUERY login_1: SELECT 1 as one FROM `users` WHERE `users`.`username` = ?;

-- endpoints/mod.rs
QUERY notif_1: SELECT BOUNDARY_notifications.notifications FROM BOUNDARY_notifications WHERE BOUNDARY_notifications.user_id = ?;
QUERY notif_2: SELECT `keystores`.* FROM `keystores` WHERE `keystores`.`key` = ?;

-- endpoints/comment.rs
QUERY comment_1: SELECT `stories`.* FROM `stories` WHERE `stories`.`short_id` = ?;
QUERY comment_2: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
QUERY comment_3: SELECT  `comments`.* FROM `comments` WHERE `comments`.`story_id` = ? AND `comments`.`short_id` = ?;
QUERY comment_4: SELECT  1 AS one FROM `comments` WHERE `comments`.`short_id` = ?;
QUERY comment_5: SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` = ?;

-- endpoints/comment_vote.rs
QUERY comment_vote_1: SELECT `comments`.* FROM `comments` WHERE `comments`.`short_id` = ?;
QUERY comment_vote_2: SELECT  `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` = ?;

-- endpoints/comments.rs
QUERY comments_1: SELECT  `comments`.id FROM `comments` WHERE `comments`.`is_deleted` = 0 AND `comments`.`is_moderated` = 0 ORDER BY id DESC LIMIT 40 OFFSET 0;
QUERY comments_2: SELECT `comment_with_votes`.id, comment_with_votes.user_id, comment_with_votes.story_id FROM `comment_with_votes` WHERE comment_with_votes.id = ?;
QUERY comments_3: SELECT 1 FROM hidden_stories WHERE user_id = ? AND hidden_stories.story_id = ?;
QUERY comments_4: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
QUERY comments_5: SELECT  `story_with_votes`.user_id FROM `story_with_votes` WHERE `story_with_votes`.`id` = ?;
QUERY comments_6: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`comment_id` = ?;
QUERY comments_7: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;

-- endpoints/frontpage.rs
QUERY frontpage_1: SELECT `frontpage_ids`.id FROM `frontpage_ids`;
QUERY frontpage_2: SELECT `story_with_votes`.user_id FROM `story_with_votes` WHERE `story_with_votes`.`id` = ? AND `story_with_votes`.`merged_story_id` IS NULL AND `story_with_votes`.`is_expired` = 0 AND `story_with_votes`.`score` >= 0;
QUERY frontpage_3: SELECT `tag_filters`.* FROM `tag_filters` WHERE `tag_filters`.`user_id` = ?;
QUERY frontpage_4: SELECT `taggings`.`story_id` FROM `taggings` WHERE `taggings`.`story_id` = ?;
QUERY frontpage_5: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
QUERY frontpage_6: SELECT `suggested_titles`.* FROM `suggested_titles` WHERE `suggested_titles`.`story_id` = ?;
QUERY frontpage_7: SELECT `suggested_taggings`.* FROM `suggested_taggings` WHERE `suggested_taggings`.`story_id` = ?;
QUERY frontpage_8: SELECT `taggings`.tag_id FROM `taggings` WHERE `taggings`.`story_id` = ?;
QUERY frontpage_9: SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;
QUERY frontpage_10: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
QUERY frontpage_11: SELECT `hidden_stories`.* FROM `hidden_stories` WHERE `hidden_stories`.`user_id` = ? AND `hidden_stories`.`story_id` = ?;
QUERY frontpage_12: SELECT `saved_stories`.* FROM `saved_stories` WHERE `saved_stories`.`user_id` = ? AND `saved_stories`.`story_id` = ?;

-- endpoints/recent.rs
QUERY recent_1: SELECT `stories`.id FROM `stories` WHERE `stories`.`merged_story_id` IS NULL AND `stories`.`is_expired` = 0 ORDER BY stories.id DESC LIMIT 51;
QUERY recent_2: SELECT `story_with_votes`.user_id FROM `story_with_votes` WHERE `story_with_votes`.`id` = ?;
QUERY recent_3: SELECT `tag_filters`.* FROM `tag_filters` WHERE `tag_filters`.`user_id` = ?;
QUERY recent_4: SELECT `taggings`.`story_id` FROM `taggings` WHERE `taggings`.`story_id` = ?;
QUERY recent_5: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
QUERY recent_6: SELECT `suggested_titles`.* FROM `suggested_titles` WHERE `suggested_titles`.`story_id` = ?;
QUERY recent_7: SELECT `suggested_taggings`.* FROM `suggested_taggings` WHERE `suggested_taggings`.`story_id` = ?;
QUERY recent_8: SELECT `taggings`.tag_id FROM `taggings` WHERE `taggings`.`story_id` = ?;
QUERY recent_9: SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;
QUERY recent_10: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
QUERY recent_11: SELECT `hidden_stories`.* FROM `hidden_stories` WHERE `hidden_stories`.`user_id` = ? AND `hidden_stories`.`story_id` = ?;
QUERY recent_12: SELECT `saved_stories`.* FROM `saved_stories` WHERE `saved_stories`.`user_id` = ? AND `saved_stories`.`story_id` = ?;

-- endpoints/story.rs
QUERY story_1: SELECT `story_with_votes`.* FROM `story_with_votes` WHERE `story_with_votes`.`short_id` = ?;
QUERY story_2: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
QUERY story_3: SELECT `read_ribbons`.* FROM `read_ribbons` WHERE `read_ribbons`.`user_id` = ? AND `read_ribbons`.`story_id` = ?;
QUERY story_4: SELECT `stories`.`id` FROM `stories` WHERE `stories`.`merged_story_id` = ?;
QUERY story_5: SELECT `comment_with_votes`.* FROM `comment_with_votes` WHERE `comment_with_votes`.`story_id` = ? ORDER BY comment_with_votes.score DESC;
QUERY story_6: SELECT `users`.* FROM `users` WHERE `users`.`id` = ?;
QUERY story_7: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`comment_id` = ?;
QUERY story_8: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
QUERY story_9: SELECT `hidden_stories`.* FROM `hidden_stories` WHERE `hidden_stories`.`user_id` = ? AND `hidden_stories`.`story_id` = ?;
QUERY story_10: SELECT `saved_stories`.* FROM `saved_stories` WHERE `saved_stories`.`user_id` = ? AND `saved_stories`.`story_id` = ?;
QUERY story_11: SELECT `taggings`.tag_id FROM `taggings` WHERE `taggings`.`story_id` = ?;
QUERY story_12: SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;

-- endpoints/user.rs
QUERY user_1: SELECT `users`.* FROM `users` WHERE `users`.`username` = ?;
QUERY user_2: SELECT user_stats.* FROM user_stats WHERE user_stats.id = ?;
QUERY user_3: SELECT `tags`.`id`, COUNT(*) AS `count` FROM `taggings` INNER JOIN `tags` ON `taggings`.`tag_id` = `tags`.`id` INNER JOIN `stories` ON `stories`.`id` = `taggings`.`story_id` WHERE `tags`.`inactive` = 0 AND `stories`.`user_id` = ? GROUP BY `tags`.`id` ORDER BY `count` desc LIMIT 1;
QUERY user_4: SELECT `tags`.* FROM `tags` WHERE `tags`.`id` = ?;
QUERY user_5: SELECT 1 AS one FROM `hats` WHERE `hats`.`user_id` = ? LIMIT 1;

-- endpoints/story_vote.rs
QUERY story_vote_1: SELECT `stories`.* FROM `stories` WHERE `stories`.`short_id` = ?;
QUERY story_vote_2: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;

-- endpoints/submit.rs
QUERY submit_1: SELECT `tags`.* FROM `tags` WHERE `tags`.`inactive` = 0 AND `tags`.`tag` IN ('test');
QUERY submit_2: SELECT 1 AS one FROM `stories` WHERE `stories`.`short_id` = ?;
QUERY submit_3: SELECT `votes`.* FROM `votes` WHERE `votes`.`user_id` = ? AND `votes`.`story_id` = ? AND `votes`.`comment_id` IS NULL;
