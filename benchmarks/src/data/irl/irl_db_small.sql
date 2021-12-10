CREATE TABLE `_groups_del` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int unsigned NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL,
  `suggested_group_id` int DEFAULT NULL,
  `image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `key` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `organization_id` int DEFAULT NULL,
  `is_official_for_org` int NOT NULL DEFAULT '0',
  `can_send_invites` tinyint NOT NULL DEFAULT '1',
  `description` text COLLATE utf8mb4_unicode_ci,
  `add_all_org_members` tinyint NOT NULL DEFAULT '0',
  `is_paid` tinyint NOT NULL DEFAULT '0',
  `is_restricted` tinyint(1) NOT NULL DEFAULT '0',
  `other_user_id` int DEFAULT NULL,
  `is_public` tinyint(1) NOT NULL DEFAULT '0',
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chat_messages_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `chat_images_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `chat_polls_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `is_request_to_join` tinyint NOT NULL DEFAULT '0',
  `is_approved_for_explore` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `other_user_id_user_id_unique` (`other_user_id`,`user_id`),
  KEY `user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `_users_del` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `role_id` bigint DEFAULT NULL,
  `first` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `username` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso_code` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phone` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `picture` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `code` varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `signed_up` tinyint(1) DEFAULT '0',
  `key` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `granted_notifs` tinyint(1) DEFAULT '0',
  `granted_contacts` tinyint(1) DEFAULT '0',
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `timezone` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_session` datetime DEFAULT NULL,
  `location_hidden` tinyint(1) DEFAULT '0',
  `is_deleted` tinyint(1) DEFAULT '0',
  `birthday` date DEFAULT NULL,
  `gender` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_type` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_id` int DEFAULT NULL,
  `grade` int DEFAULT NULL,
  `signup_date` datetime DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_verified` tinyint NOT NULL DEFAULT '0',
  `bio` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_public` tinyint NOT NULL DEFAULT '0',
  `organization_id` int DEFAULT NULL,
  `ip_address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_older_than_thirteen` tinyint NOT NULL DEFAULT '0',
  `profile_background` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `app_download_token` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_name` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_login` timestamp NULL DEFAULT NULL,
  `permissions` json DEFAULT NULL,
  `encrypted_email` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `encrypted_phone` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `password` varchar(60) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `phone` (`phone`),
  UNIQUE KEY `users_username_unique` (`username`),
  UNIQUE KEY `users_email_unique` (`email`),
  UNIQUE KEY `app_download_token` (`app_download_token`) USING BTREE,
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `api_content_provider_name_id_composite_index` (`api_content_provider_name`,`api_content_provider_id`),
  KEY `last_session` (`last_session`),
  KEY `signup_date` (`signup_date`),
  KEY `timezone` (`timezone`),
  KEY `picture` (`picture`),
  KEY `updated_at` (`updated_at`)
) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `_users_groups_del` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `group_id` int unsigned NOT NULL,
  `inviter_id` int unsigned NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL,
  `is_seen` tinyint(1) NOT NULL DEFAULT '0',
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `pending` tinyint NOT NULL DEFAULT '0',
  `joined_at` datetime DEFAULT NULL,
  `last_opened_at` datetime DEFAULT NULL,
  `mute_until` datetime DEFAULT NULL,
  `denied_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_group_id_index` (`user_id`,`group_id`),
  KEY `users_groups_joined_at_index` (`joined_at`),
  KEY `inviter_id_index` (`inviter_id`),
  KEY `group_id_user_id` (`group_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `ab_tests` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `num_treatments` int unsigned NOT NULL,
  `deploy_percentage` int unsigned NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `ab_tests_users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint unsigned NOT NULL,
  `ab_test_id` bigint unsigned NOT NULL,
  `treatment` int unsigned NOT NULL,
  `override` tinyint(1) NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ab_test_id_user_id_unique_index` (`ab_test_id`,`user_id`),
  KEY `user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `api_content_provider_user_subscriptions` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `api_content_provider_id` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  `api_content_provider_name` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
  `api_content_provider_username` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `explore_category` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  `explore_category_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `weekday` tinyint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `api_content_providers` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint DEFAULT NULL,
  `invite_id` bigint DEFAULT NULL,
  `api_content_provider_sort_metric_raw` bigint DEFAULT NULL,
  `api_content_provider_sort_metric_normalized` double(8,5) DEFAULT NULL,
  `api_content_provider_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_name` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `invite_id_api_content_provider_name_id_composite_index` (`invite_id`,`api_content_provider_name`,`api_content_provider_id`),
  UNIQUE KEY `user_id_api_content_provider_name_id_composite_index` (`user_id`,`api_content_provider_name`,`api_content_provider_id`),
  KEY `api_content_provider_name_id_composite_index` (`api_content_provider_name`,`api_content_provider_id`)
) ENGINE=InnoDB AUTO_INCREMENT=50 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `ar_internal_metadata` (
  `key` varchar(255) NOT NULL,
  `value` varchar(255) DEFAULT NULL,
  `created_at` datetime NOT NULL,
  `updated_at` datetime NOT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `block_user_reasons` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `note` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `chats` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `description` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `table_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `row_id` int NOT NULL,
  `chat_sent_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `firebase_key` varchar(25) COLLATE utf8mb4_unicode_ci NOT NULL,
  `key` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `row_id_table_name_unique` (`row_id`,`table_name`),
  UNIQUE KEY `chats_key_unique` (`key`),
  KEY `chats_user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `cities_to_seed` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `city` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `state` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `last_seeded_eventful` datetime NOT NULL,
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `last_seeded_meetup` datetime NOT NULL,
  `geo_hash` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_seeded_ticketmaster` datetime DEFAULT NULL,
  `last_seeded_eventbrite` datetime NOT NULL,
  `country` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `city_state_composite_index` (`city`,`state`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `cont` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `cont_trigger` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `copies_and_preferences` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `app_version_ios` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `app_version_android` varchar(12) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `update_app_copy` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `force_update_app` tinyint(1) DEFAULT '0',
  `update_app_button_copy` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `update_app_skip_copy` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `update_app_title_copy` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `select_all_during_onboarding` tinyint(1) DEFAULT NULL,
  `show_skip_onboarding` tinyint(1) DEFAULT NULL,
  `nom_segment_count` int NOT NULL,
  `min_between_nom_segments` int NOT NULL,
  `use_default_auth` tinyint NOT NULL DEFAULT '0',
  `phone_number_agreement_copy` text COLLATE utf8mb4_unicode_ci,
  `phone_number_agreement_bolded_copy` text COLLATE utf8mb4_unicode_ci,
  `phone_number_agreement_hyperlinked_copy` text COLLATE utf8mb4_unicode_ci,
  `phone_number_agreement_hyperlinked_link` text COLLATE utf8mb4_unicode_ci,
  `sms_explanation_copy` text COLLATE utf8mb4_unicode_ci,
  `sms_explanation_bolded_copy` text COLLATE utf8mb4_unicode_ci,
  `sms_explanation_hyperlinked_copy` text COLLATE utf8mb4_unicode_ci,
  `sms_explanation_hyperlinked_link` text COLLATE utf8mb4_unicode_ci,
  `use_elasticsearch_for_invites` tinyint NOT NULL DEFAULT '0',
  `use_elasticsearch_for_home_calendar` tinyint DEFAULT '0',
  `re_sync_google_before_this_date` datetime NOT NULL,
  `use_safe_following_feed_query` tinyint NOT NULL DEFAULT '0',
  `max_calendar_callback_count` tinyint NOT NULL DEFAULT '50',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `counters` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint NOT NULL,
  `follower_count` bigint NOT NULL,
  `following_count` bigint NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `data_rows` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `data_type_id` int unsigned NOT NULL,
  `field` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `display_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `required` tinyint(1) NOT NULL DEFAULT '0',
  `browse` tinyint(1) NOT NULL DEFAULT '1',
  `read` tinyint(1) NOT NULL DEFAULT '1',
  `edit` tinyint(1) NOT NULL DEFAULT '1',
  `add` tinyint(1) NOT NULL DEFAULT '1',
  `delete` tinyint(1) NOT NULL DEFAULT '1',
  `details` text COLLATE utf8mb4_unicode_ci,
  PRIMARY KEY (`id`),
  KEY `data_rows_data_type_id_foreign` (`data_type_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `data_types` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `slug` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `display_name_singular` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `display_name_plural` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `icon` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `model_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `generate_permissions` tinyint(1) NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `data_types_name_unique` (`name`),
  UNIQUE KEY `data_types_slug_unique` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `default_invite_chats` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `chat` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `invite_id` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `default_invites` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `suggested_invite_id` int DEFAULT NULL,
  `title` text COLLATE utf8mb4_unicode_ci,
  `color` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `start_time_set` tinyint(1) DEFAULT '0',
  `end_time_set` tinyint(1) DEFAULT '0',
  `invite_filter_id` int DEFAULT NULL,
  `show_creator` tinyint(1) DEFAULT '1',
  `type` tinyint(1) DEFAULT '1',
  `latitude` double(18,12) DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `location_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reply_comment` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `initial_comment` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `default_recurring_invites_data` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `start_time` datetime DEFAULT NULL,
  `by_day` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `length_in_minutes` int NOT NULL DEFAULT '0',
  `start_time_set` tinyint(1) DEFAULT '0',
  `frequency` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=77 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `default_titles` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `default_recurring_invite_data_id` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `title_index` (`title`),
  KEY `default_recurring_invite_data_id_index` (`default_recurring_invite_data_id`)
) ENGINE=InnoDB AUTO_INCREMENT=64 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `deleted_users` (
  `user_id` bigint unsigned NOT NULL,
  `json` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `reason` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `department_locations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `country` varchar(32) DEFAULT NULL,
  `department_id` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=latin1;


CREATE TABLE `departments` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=latin1;


CREATE TABLE `dms` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `key` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `user_id` bigint NOT NULL,
  `name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `user_id_larger` bigint unsigned NOT NULL,
  `user_id_smaller` bigint unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `dms_key_unique` (`key`),
  UNIQUE KEY `user_id_larger_smaller_unique_index` (`user_id_larger`,`user_id_smaller`),
  KEY `user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=81 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `dms_users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `mute_until` datetime DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `seen_at` datetime DEFAULT NULL,
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `user_id` bigint NOT NULL,
  `dm_id` bigint NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_dm_id_unique` (`user_id`,`dm_id`),
  KEY `dm_id_index` (`dm_id`)
) ENGINE=InnoDB AUTO_INCREMENT=139 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `edus` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `alpha_two_code` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state-province` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `domains` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `logo` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=150 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `employees` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `department_id` int DEFAULT NULL,
  `salary` int DEFAULT NULL,
  `name` varchar(32) DEFAULT NULL,
  `kind` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=latin1;


CREATE TABLE `explore_section_sub_categories` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `sub_category` varchar(60) COLLATE utf8mb4_unicode_ci NOT NULL,
  `explore_section_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `explore_section_id_sub_category_unique_composite_index` (`explore_section_id`,`sub_category`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `explore_sections` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  `section_key` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  `gradient_start` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `gradient_end` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `users_can_post` tinyint NOT NULL DEFAULT '0',
  `is_active` tinyint(1) NOT NULL DEFAULT '0',
  `organization_id` int DEFAULT NULL,
  `web_image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'default',
  `priority` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `name` (`name`),
  KEY `section_key` (`section_key`),
  KEY `organization_id` (`organization_id`)
) ENGINE=InnoDB AUTO_INCREMENT=292 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_accounts` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `is_default` tinyint(1) NOT NULL DEFAULT '0',
  `external_user_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `type` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `access_token` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `expire_token` bigint NOT NULL,
  `refresh_token` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `needs_re_authorization` tinyint NOT NULL DEFAULT '0',
  `should_email_people_in_events` tinyint NOT NULL DEFAULT '0',
  `callback_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_token_expiration` bigint NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `add_irl_message_in_all_events` tinyint NOT NULL DEFAULT '0',
  `first` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `phone` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_full_sync_start_date` datetime DEFAULT NULL,
  `last_full_sync_completion_date` datetime DEFAULT NULL,
  `organization_id` int DEFAULT NULL,
  `first_access_token` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `did_move_callback` tinyint NOT NULL DEFAULT '0',
  `key_file_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `two_way_sync_enabled` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `email_index` (`email`) USING BTREE,
  UNIQUE KEY `organization_id_user_id_index` (`organization_id`,`user_id`),
  KEY `user_id_index` (`user_id`),
  KEY `key_file_name_index` (`key_file_name`)
) ENGINE=InnoDB AUTO_INCREMENT=50 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_calendars` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `external_account_id` bigint unsigned NOT NULL,
  `external_calendar_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `last_sync` timestamp NOT NULL,
  `is_enabled` tinyint NOT NULL DEFAULT '1',
  `primary` tinyint NOT NULL DEFAULT '0',
  `next_sync_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_expiration` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `should_skip_callback` tinyint NOT NULL DEFAULT '0',
  `user_id_subscribing_to` bigint DEFAULT NULL,
  `is_irl_cal` tinyint DEFAULT NULL,
  `access_type` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'reader',
  PRIMARY KEY (`id`),
  UNIQUE KEY `external_calendar_id_external_account_id_index` (`external_calendar_id`,`external_account_id`),
  UNIQUE KEY `user_id_external_calendar_id` (`user_id`,`external_calendar_id`),
  UNIQUE KEY `user_id_subscribing_to_user_id_index` (`user_id_subscribing_to`,`user_id`),
  UNIQUE KEY `user_id_is_irl_cal_unique_index` (`user_id`,`is_irl_cal`),
  KEY `external_account_id_index` (`external_account_id`),
  KEY `callback_id_callback_token_index` (`callback_id`,`callback_token`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_invite_attendees` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `external_invite_id` int unsigned DEFAULT NULL,
  `original_email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `irl_calendar_email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `external_event_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ext_invite_email_unique` (`external_invite_id`,`original_email`,`irl_calendar_email`),
  KEY `attendee_external_invite_id_index` (`external_invite_id`),
  KEY `attendee_external_event_id_index` (`external_event_id`),
  KEY `external_event_id_index` (`external_event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_invites` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `invite_id` int unsigned NOT NULL,
  `external_calendar_id` bigint unsigned NOT NULL,
  `external_event_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `just_modified_in_irl` tinyint NOT NULL DEFAULT '0',
  `type` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'google',
  `sync_status` tinyint NOT NULL DEFAULT '0',
  `sequence` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `invite_id_type_unique` (`invite_id`,`type`),
  UNIQUE KEY `external_invites_external_event_id_unique` (`external_event_id`),
  KEY `external_calendar_id_index` (`external_calendar_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_invites_private` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `invite_id` int unsigned NOT NULL,
  `external_calendar_id` bigint unsigned NOT NULL,
  `external_account_id` int unsigned NOT NULL,
  `external_event_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `invite_id_external_calendar_id_index` (`invite_id`,`external_calendar_id`),
  UNIQUE KEY `external_invites_private_external_event_id_unique` (`external_event_id`),
  KEY `external_event_id_index` (`external_event_id`)
) ENGINE=InnoDB AUTO_INCREMENT=345 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_product_payments` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `payment_account_id` int NOT NULL,
  `external_product_id` int NOT NULL,
  `payment_stopped_at` datetime DEFAULT NULL,
  `payment_stopped_reason` tinyint DEFAULT NULL,
  `external_product_payment_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `platform_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `current_period_start_at` datetime DEFAULT NULL,
  `current_period_end_at` datetime DEFAULT NULL,
  `cancel_at_period_end` tinyint NOT NULL DEFAULT '0',
  `canceled_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `external_product_id_payment_account_id_index` (`external_product_id`,`payment_account_id`),
  UNIQUE KEY `external_product_payment_platform_name_id_unique` (`external_product_payment_id`,`platform_name`) USING BTREE,
  KEY `payment_account_id_index` (`payment_account_id`)
) ENGINE=InnoDB AUTO_INCREMENT=143 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_product_prices` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `price_in_cents` int unsigned NOT NULL,
  `currency` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `payment_type` tinyint NOT NULL,
  `external_product_id` int unsigned NOT NULL,
  `external_product_price_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `external_product_price_id_unique` (`external_product_price_id`),
  KEY `external_product_id_index` (`external_product_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `external_products` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `payment_account_id` int NOT NULL,
  `price_in_cents` int NOT NULL,
  `currency` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `payment_type` tinyint NOT NULL,
  `table_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `row_id` int NOT NULL,
  `external_product_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `external_price_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_for_acceptance` tinyint DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `external_product_id_unique` (`external_product_id`),
  UNIQUE KEY `row_id_table_name_is_for_acceptance_unique` (`row_id`,`table_name`,`is_for_acceptance`),
  KEY `payment_account_id_index` (`payment_account_id`)
) ENGINE=InnoDB AUTO_INCREMENT=204 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `extra_user_data` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `last_email_code_sent_date` datetime NOT NULL,
  `last_email_signin_date` datetime DEFAULT NULL,
  `did_populate_all_chats` tinyint NOT NULL DEFAULT '0',
  `merged_from_user_id` int DEFAULT NULL,
  `download_attribution_key` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `extra_user_data_user_id_unique` (`user_id`),
  KEY `extra_user_data_download_attribution_key_index` (`download_attribution_key`)
) ENGINE=InnoDB AUTO_INCREMENT=4000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `failed_jobs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `connection` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `queue` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `payload` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `exception` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `failed_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `featured_content_suggestions` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `table_name` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
  `table_row_id` bigint NOT NULL,
  `is_active` tinyint NOT NULL DEFAULT '0',
  `priority` int NOT NULL DEFAULT '0',
  `content_image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `table_name_table_row_id_index` (`table_name`,`table_row_id`)
) ENGINE=InnoDB AUTO_INCREMENT=76 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `filtered_phrases` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `phrase` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `gestures` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reply_gesture_id` int DEFAULT NULL,
  `notification_copy` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `gather_notification_copy` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `bolded_string` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `active_image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `inactive_image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `json_animation` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT '1',
  `is_default` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `google_accounts` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `first` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `phone` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `is_default` tinyint NOT NULL DEFAULT '0',
  `access_token` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `server_auth_code` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `needs_re_authorization` tinyint NOT NULL DEFAULT '0',
  `callback_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `callback_token_expiration` int NOT NULL DEFAULT '0',
  `should_email_people_in_events` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `google_accounts_email_unique` (`email`),
  UNIQUE KEY `email_index` (`email`),
  UNIQUE KEY `google_accounts_callback_id_unique` (`callback_id`),
  UNIQUE KEY `google_accounts_callback_token_unique` (`callback_token`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=249 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `gradients` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `tile_background` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `full_screen_background` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `invite_circle` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `profile` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `greetings` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `group_rules` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(75) COLLATE utf8mb4_unicode_ci NOT NULL,
  `description` varchar(225) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `priority` tinyint NOT NULL,
  `group_id` bigint unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id_title_unique_index` (`group_id`,`title`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `groups` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `user_id` int unsigned NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL,
  `suggested_group_id` int DEFAULT NULL,
  `image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `key` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `organization_id` int DEFAULT NULL,
  `is_official_for_org` int NOT NULL DEFAULT '0',
  `can_send_invites` tinyint NOT NULL DEFAULT '1',
  `description` text COLLATE utf8mb4_unicode_ci,
  `add_all_org_members` tinyint NOT NULL DEFAULT '0',
  `is_paid` tinyint NOT NULL DEFAULT '0',
  `is_restricted` tinyint(1) NOT NULL DEFAULT '0',
  `other_user_id` int DEFAULT NULL,
  `is_public` tinyint(1) NOT NULL DEFAULT '0',
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `chat_messages_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `chat_images_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `chat_polls_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `is_request_to_join` tinyint NOT NULL DEFAULT '0',
  `is_approved_for_explore` tinyint NOT NULL DEFAULT '0',
  `is_name_auto_generated` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `other_user_id_user_id_unique` (`other_user_id`,`user_id`),
  KEY `user_id_index` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `groups_hashtags` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `hashtag_id` bigint unsigned NOT NULL,
  `group_id` bigint unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `groups_role_grants` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` bigint unsigned NOT NULL,
  `granter_id` bigint unsigned NOT NULL,
  `entity_id` bigint unsigned NOT NULL,
  `role_name` varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_composite_index` (`entity_id`,`user_id`,`role_name`),
  KEY `user_id_entity_id_index` (`user_id`,`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `hackers` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(10) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;


CREATE TABLE `hashtags` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `image` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_index` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=2000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `holidays` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  `date` datetime NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=26 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `interests` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `interests_sub_interests` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `interest_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `sub_interest_id` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `invalid_email_domains` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `email_domain` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email_domain_unique` (`email_domain`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `invite_filters` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `featured_image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `priority` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `is_default` tinyint NOT NULL DEFAULT '0',
  `card_image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `full_screen_image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `circle_image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `icon_image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `unsplash_image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=175 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `invite_keywords` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `keyword` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `invites` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `key` varchar(8) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `start_date` datetime DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `start_time_set` tinyint(1) DEFAULT '0',
  `end_time_set` tinyint(1) DEFAULT '0',
  `type` tinyint(1) DEFAULT '1',
  `is_deleted` tinyint(1) DEFAULT '0',
  `is_expired` tinyint(1) DEFAULT '0',
  `suggested_invite_id` int DEFAULT NULL,
  `is_default` tinyint(1) DEFAULT '0',
  `show_creator` tinyint(1) DEFAULT '1',
  `latitude` double(18,12) DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `location_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `postal_code` varchar(11) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `recurring_type` tinyint NOT NULL DEFAULT '0',
  `image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` text COLLATE utf8mb4_unicode_ci,
  `link` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_reported` tinyint NOT NULL DEFAULT '0',
  `parent_invite_id` int DEFAULT NULL,
  `google_calendar_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `google_calendar_recurring_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_personal` tinyint NOT NULL DEFAULT '0',
  `is_private` tinyint(1) NOT NULL DEFAULT '0',
  `is_public` tinyint NOT NULL DEFAULT '0',
  `from_google_calendar` tinyint NOT NULL DEFAULT '0',
  `organization_id` int DEFAULT NULL,
  `recurring_invite_id` int DEFAULT NULL,
  `recurring_invite_index` int DEFAULT NULL,
  `is_recurring_invite_template` tinyint NOT NULL DEFAULT '0',
  `is_removed_from_recurring` tinyint NOT NULL DEFAULT '0',
  `is_modified_from_recurring_template` tinyint NOT NULL DEFAULT '0',
  `invitees_can_modify` tinyint NOT NULL DEFAULT '1',
  `initial_start_date` datetime DEFAULT NULL,
  `initial_end_date` datetime DEFAULT NULL,
  `is_chat_disabled` tinyint NOT NULL DEFAULT '0',
  `are_actions_hidden` tinyint NOT NULL DEFAULT '0',
  `are_people_hidden` tinyint NOT NULL DEFAULT '0',
  `is_creator_hidden` tinyint NOT NULL DEFAULT '0',
  `is_for_followers` tinyint NOT NULL DEFAULT '0',
  `language` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ticketmaster_min_price_with_fees` double(8,2) DEFAULT NULL,
  `ticketmaster_max_price_with_fees` double(8,2) DEFAULT NULL,
  `ticketmaster_currency` varchar(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ticketmaster_hot_event` tinyint DEFAULT '0',
  `ticketmaster_onsale_start_datetime` datetime DEFAULT NULL,
  `ticketmaster_onsale_end_datetime` datetime DEFAULT NULL,
  `ticketmaster_presale_start_datetime` datetime DEFAULT NULL,
  `ticketmaster_presale_end_datetime` datetime DEFAULT NULL,
  `ticketmaster_min_price` double(8,2) DEFAULT NULL,
  `ticketmaster_max_price` double(8,2) DEFAULT NULL,
  `api_content_provider_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_name` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_sort_metric_raw` bigint DEFAULT NULL,
  `api_content_provider_sort_metric_normalized` double(6,5) DEFAULT NULL,
  `can_be_modified` tinyint NOT NULL DEFAULT '1',
  `deeplink` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `only_creator_can_modify` tinyint NOT NULL DEFAULT '0',
  `is_restricted` tinyint NOT NULL DEFAULT '0',
  `chat_messages_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `chat_images_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  `chat_polls_disabled_for_members` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `recurring_invite_id_and_recurring_invite_index_index` (`recurring_invite_id`,`recurring_invite_index`),
  UNIQUE KEY `api_content_provider_name_id_composite_index` (`api_content_provider_name`,`api_content_provider_id`),
  UNIQUE KEY `google_calendar_id_unique` (`google_calendar_id`),
  KEY `user_id` (`user_id`),
  KEY `date` (`date`),
  KEY `start_date` (`start_date`),
  KEY `updated_at` (`updated_at`),
  KEY `edu_id` (`organization_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `invites_groups` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `invite_id` int unsigned NOT NULL,
  `group_id` int unsigned NOT NULL,
  `inviter_id` int unsigned NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `invite_id_group_id_index` (`invite_id`,`group_id`),
  KEY `group_id_index` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2074 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `invites_hashtags` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `invite_id` int DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `hashtag_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_composite_index` (`hashtag_id`,`invite_id`),
  KEY `invite_id` (`invite_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `invites_reported` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `key` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `date` datetime DEFAULT NULL,
  `start_time_set` tinyint(1) DEFAULT '0',
  `end_time_set` tinyint(1) DEFAULT '0',
  `type` tinyint(1) DEFAULT '1',
  `is_deleted` tinyint(1) DEFAULT '0',
  `is_expired` tinyint(1) DEFAULT '0',
  `invite_filter_id` int DEFAULT NULL,
  `suggested_invite_id` int DEFAULT NULL,
  `gradient_id` int DEFAULT '0',
  `is_default` tinyint(1) DEFAULT '0',
  `num_soon_reminders` tinyint(1) DEFAULT '0',
  `show_creator` tinyint(1) DEFAULT '1',
  `latitude` double(18,12) DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `location_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `city` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `postal_code` varchar(11) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `address` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `recurring_type` tinyint NOT NULL DEFAULT '0',
  `hide_reprompt` tinyint(1) DEFAULT '0',
  `image` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `link` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_reported` tinyint NOT NULL DEFAULT '0',
  `parent_invite_id` int DEFAULT NULL,
  `meetup_id` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ticketmaster_id` varchar(155) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `eventbrite_id` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `google_calendar_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `google_calendar_recurring_id` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_personal` tinyint NOT NULL DEFAULT '0',
  `is_private` tinyint(1) NOT NULL DEFAULT '0',
  `is_public` tinyint NOT NULL DEFAULT '0',
  `school_id` int NOT NULL,
  `user_calendar_id` int NOT NULL,
  `from_google_calendar` tinyint NOT NULL DEFAULT '0',
  `edu_id` int NOT NULL,
  `recurring_invite_id` int DEFAULT NULL,
  `recurring_invite_index` int DEFAULT NULL,
  `is_recurring_invite_template` tinyint NOT NULL DEFAULT '0',
  `is_removed_from_recurring` tinyint NOT NULL DEFAULT '0',
  `is_modified_from_recurring_template` tinyint NOT NULL DEFAULT '0',
  `invitees_can_modify` tinyint NOT NULL DEFAULT '1',
  `initial_start_date` datetime DEFAULT NULL,
  `initial_end_date` datetime DEFAULT NULL,
  `is_chat_disabled` tinyint NOT NULL DEFAULT '0',
  `are_actions_hidden` tinyint NOT NULL DEFAULT '0',
  `are_people_hidden` tinyint NOT NULL DEFAULT '0',
  `is_creator_hidden` tinyint NOT NULL DEFAULT '0',
  `did_post_sms_email_chat_explanation` tinyint NOT NULL DEFAULT '0',
  `themoviedb_id` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_for_followers` tinyint NOT NULL DEFAULT '0',
  `language` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ticketmaster_min_price_with_fees` double(8,2) DEFAULT NULL,
  `ticketmaster_max_price_with_fees` double(8,2) DEFAULT NULL,
  `ticketmaster_currency` varchar(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ticketmaster_hot_event` tinyint DEFAULT '0',
  `ticketmaster_onsale_start_datetime` datetime DEFAULT NULL,
  `ticketmaster_onsale_end_datetime` datetime DEFAULT NULL,
  `ticketmaster_presale_start_datetime` datetime DEFAULT NULL,
  `ticketmaster_presale_end_datetime` datetime DEFAULT NULL,
  `ticketmaster_min_price` double(8,2) DEFAULT NULL,
  `ticketmaster_max_price` double(8,2) DEFAULT NULL,
  `themoviedb_popularity` decimal(7,3) DEFAULT NULL,
  `api_content_provider_id` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_name` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_sort_metric_raw` bigint DEFAULT NULL,
  `api_content_provider_sort_metric_normalized` double(6,5) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `invites_meetup_id_unique` (`meetup_id`),
  UNIQUE KEY `invites_eventbrite_id_unique` (`eventbrite_id`),
  UNIQUE KEY `invites_google_calendar_id_unique` (`google_calendar_id`),
  UNIQUE KEY `recurring_invite_id_and_recurring_invite_index_index` (`recurring_invite_id`,`recurring_invite_index`),
  UNIQUE KEY `invites_themoviedb_id_unique` (`themoviedb_id`),
  UNIQUE KEY `invites_ticketmaster_id_unique` (`ticketmaster_id`),
  UNIQUE KEY `parent_invite_id_user_id_unique_index` (`parent_invite_id`,`user_id`),
  UNIQUE KEY `api_content_provider_name_id_composite_index` (`api_content_provider_name`,`api_content_provider_id`),
  KEY `user_id` (`user_id`),
  KEY `date` (`date`),
  KEY `state_city_index` (`state`,`city`),
  KEY `start_date` (`start_date`),
  KEY `themoviedb_id` (`themoviedb_id`),
  KEY `updated_at` (`updated_at`),
  FULLTEXT KEY `title_full` (`title`)
) ENGINE=InnoDB AUTO_INCREMENT=4000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `invites_users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `group_id` int DEFAULT NULL,
  `invite_id` int NOT NULL,
  `attending` tinyint(1) DEFAULT '0',
  `pending` tinyint(1) DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `inviter_id` int DEFAULT NULL,
  `did_tap_invite` tinyint(1) DEFAULT '0',
  `did_tap_next_time` tinyint(1) DEFAULT '0',
  `joined_at` datetime DEFAULT NULL,
  `did_send_seen_notif` tinyint(1) DEFAULT '0',
  `seen_at` datetime DEFAULT NULL,
  `last_opened_at` datetime DEFAULT NULL,
  `did_opt_out_of_recurring` tinyint(1) DEFAULT '0',
  `wants_to_repeat` tinyint NOT NULL DEFAULT '0',
  `removed` tinyint DEFAULT '0',
  `interested` tinyint NOT NULL DEFAULT '0',
  `did_report` tinyint NOT NULL DEFAULT '0',
  `updated_at_for_feed` datetime NOT NULL,
  `invited_at` datetime DEFAULT NULL,
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `banned` tinyint NOT NULL DEFAULT '0',
  `is_host` tinyint DEFAULT '0',
  `uninvited` tinyint NOT NULL DEFAULT '0',
  `added_from_subscribe` tinyint NOT NULL DEFAULT '0',
  `mute_until` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_invite_id_index` (`user_id`,`invite_id`),
  KEY `inviter_id` (`inviter_id`),
  KEY `updated_at` (`updated_at`),
  KEY `invite_id_user_id` (`invite_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=4000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `jobs` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `queue` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `payload` longtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `attempts` tinyint unsigned NOT NULL,
  `reserved_at` int unsigned DEFAULT NULL,
  `available_at` int unsigned NOT NULL,
  `created_at` int unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `jobs_queue_index` (`queue`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `linked_social_accounts` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `provider_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `provider_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `access_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `refresh_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `provider_id` (`provider_id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=44 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `menu_items` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `menu_id` int unsigned DEFAULT NULL,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `url` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `target` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '_self',
  `icon_class` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `color` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `parent_id` int DEFAULT NULL,
  `order` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `menu_items_menu_id_foreign` (`menu_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `menus` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `menus_name_unique` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `migrations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `migration` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `batch` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=796 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `mutual_follow_groups` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id_larger` int NOT NULL,
  `user_id_smaller` int NOT NULL,
  `group_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_larger_smaller_unique_index` (`user_id_larger`,`user_id_smaller`),
  UNIQUE KEY `group_id_unique_index` (`group_id`),
  KEY `user_id_smaller_index` (`user_id_smaller`)
) ENGINE=InnoDB AUTO_INCREMENT=87 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `name` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `phone` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `google_access_token` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `google_server_auth_code` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `google_accounts_email_unique` (`email`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `new_user_emails` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `new_user_emails_user_id_unique` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `nominations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `question` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `title` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `image` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `share_image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT '1',
  `is_default` tinyint(1) NOT NULL DEFAULT '0',
  `gradient_id` int DEFAULT NULL,
  `sub_text` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `invite_filter_id` int DEFAULT NULL,
  `is_invite_nom` tinyint DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=224 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `nominations_interests_joined` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `nomination_id` int DEFAULT NULL,
  `interest_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `notification_banner_suggestions` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `above_below` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `temperature` int NOT NULL,
  `weather_type` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `suggestion` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `suggested_invite_id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `organizations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `alpha_two_code` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `state-province` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `domains` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `country` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `logo` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `type` tinyint DEFAULT '0',
  `content_level` tinyint(1) NOT NULL DEFAULT '0',
  `slug` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `organizations_user_id_unique` (`user_id`),
  UNIQUE KEY `unique_domains_index` (`domains`),
  UNIQUE KEY `organizations_slug_unique` (`slug`),
  KEY `organizations_domains_index` (`domains`)
) ENGINE=InnoDB AUTO_INCREMENT=6092 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `pages` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=latin1;


CREATE TABLE `pages_liked` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `page_id` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1;


CREATE TABLE `paid_group_seeded_data` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `group_id` int NOT NULL,
  `price_in_cents` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `was_processed` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `group_id_unique` (`group_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `payment_accounts` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `account_mode` tinyint NOT NULL,
  `platform_name` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `external_payment_account_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `status` int NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `external_payment_account_id_account_type_unique` (`external_payment_account_id`,`platform_name`),
  UNIQUE KEY `user_id_account_mode_platform_name_unique` (`user_id`,`account_mode`,`platform_name`)
) ENGINE=InnoDB AUTO_INCREMENT=675 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `plan_title_keywords` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `keyword` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL,
  `weight` int NOT NULL DEFAULT '0',
  `is_google` tinyint NOT NULL DEFAULT '0',
  `alternate_query` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `plan_title_keywords_keyword_unique` (`keyword`)
) ENGINE=InnoDB AUTO_INCREMENT=81 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `plan_title_keywords_images` (
  `plan_title_keyword_id` int NOT NULL,
  `image` varchar(155) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  UNIQUE KEY `user_id_image_composite_index` (`plan_title_keyword_id`,`image`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `promoted_explore_events` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `explore_section_key` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
  `invite_id` int NOT NULL,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `end_date` datetime NOT NULL,
  `priority` tinyint NOT NULL DEFAULT '0',
  `is_active` tinyint NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `invite_id` (`invite_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `promoted_groups_keys` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `group_key` varchar(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `key_index` (`group_key`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `promoted_users_to_follow` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `is_active` tinyint NOT NULL DEFAULT '0',
  `priority` int NOT NULL DEFAULT '0',
  `reason` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `promoted_users_to_follow_user_id_unique` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=15 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `ranked_users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `first` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `user_rank` int DEFAULT NULL,
  `db_follower_count` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `is_sports_team` tinyint(1) DEFAULT '0',
  `twitter_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  UNIQUE KEY `twitter_id` (`twitter_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5999 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `recurring_invites` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `frequency` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
  `week_start` varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `by_day` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `count` int DEFAULT NULL,
  `google_recurring_string` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `google_calendar_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `is_deleted` tinyint NOT NULL DEFAULT '0',
  `interval` tinyint NOT NULL DEFAULT '1',
  `by_month` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `recurring_invites_google_calendar_id_unique` (`google_calendar_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `recurring_invites_users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `recurring_invite_id` int NOT NULL,
  `user_id` int NOT NULL,
  `did_opt_out` tinyint NOT NULL DEFAULT '0',
  `user_calendar_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `banned` tinyint NOT NULL DEFAULT '0',
  `removed` tinyint NOT NULL DEFAULT '0',
  `attending` tinyint NOT NULL DEFAULT '0',
  `interested` tinyint NOT NULL DEFAULT '0',
  `added_from_subscribe` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_recurring_invite_id_unique` (`user_id`,`recurring_invite_id`),
  KEY `recurring_invite_id_index` (`recurring_invite_id`)
) ENGINE=InnoDB AUTO_INCREMENT=771 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `role_users` (
  `user_id` bigint unsigned NOT NULL,
  `role_id` int unsigned NOT NULL,
  PRIMARY KEY (`user_id`,`role_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `roles` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `slug` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `permissions` json DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `roles_slug_unique` (`slug`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `schema_migrations` (
  `version` varchar(255) NOT NULL,
  PRIMARY KEY (`version`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


CREATE TABLE `schools` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `city` varchar(128) DEFAULT NULL,
  `state` varchar(64) DEFAULT NULL,
  `type` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `highschool_index` (`name`,`city`,`state`)
) ENGINE=InnoDB AUTO_INCREMENT=400 DEFAULT CHARSET=latin1;


CREATE TABLE `seeded_users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `code` varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `seeded_users_code_unique` (`code`)
) ENGINE=InnoDB AUTO_INCREMENT=133 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `settings` (
  `key` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `value` json NOT NULL,
  PRIMARY KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `spotify_synced_data` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `linked_social_account_id` int DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `artist_user_id` int DEFAULT NULL,
  `artist_name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `recently_played` tinyint NOT NULL DEFAULT '0',
  `top_artist` tinyint NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `spotify_synced_data_artist_name_user_id_unique` (`artist_name`,`user_id`),
  KEY `linked_social_account_id` (`linked_social_account_id`),
  KEY `user_id` (`user_id`),
  KEY `artist_user_id` (`artist_user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=199 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `sql_queries` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `query` mediumtext COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=239 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `submissions` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `date` date DEFAULT NULL,
  `hacker_id` int DEFAULT NULL,
  `score` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25 DEFAULT CHARSET=latin1;


CREATE TABLE `suggested_groups` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `recurring_frequency` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `recurring_event_title_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci NOT NULL,
  `is_active` tinyint NOT NULL DEFAULT '1',
  `default_recurring_invite_data_id` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `title` (`title`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `suggested_invite_groups` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(60) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `priority` int DEFAULT '0',
  `sub_header` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `kind` smallint DEFAULT '0',
  `ios_version` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '1.0',
  `android_version` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '1.0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `suggested_invites` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `priority` int DEFAULT '0',
  `date` date DEFAULT NULL,
  `filter_id` int DEFAULT NULL,
  `gradient_id` int DEFAULT '1',
  `is_active` tinyint(1) DEFAULT '0',
  `hex_color` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `kind` smallint DEFAULT NULL,
  `group_id` int DEFAULT NULL,
  `global` tinyint(1) DEFAULT '1',
  `first_comment` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `latitude` double(18,12) DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `gender` varchar(1) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `giveaway_num_wins` tinyint NOT NULL DEFAULT '0',
  `giveaway_joining_limit` tinyint NOT NULL DEFAULT '0',
  `giveaway_dialogue_title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `giveaway_dialogue_description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `giveaway_win_comment` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
  `giveaway_lose_comment_appended` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_live_giveaway` tinyint NOT NULL DEFAULT '0',
  `suggested_invite_id` int DEFAULT NULL,
  `image` varchar(155) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `initialization_date` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=375 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `suggested_invites_interests` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `suggested_invite_id` int NOT NULL,
  `interest_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=66 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `suggested_invites_users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `suggested_invite_id` int DEFAULT NULL,
  `liked` tinyint NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id_suggested_invite_id` (`user_id`,`suggested_invite_id`)
) ENGINE=InnoDB AUTO_INCREMENT=69 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `suggested_subscriptions` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `category` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `sub_category` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=34 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `surfaced_suggested_invites` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `invite_id` int unsigned NOT NULL,
  `surfaced_at` datetime NOT NULL,
  `status` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `explore_section_id` int unsigned NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `reason` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=44 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `test_table` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12 DEFAULT CHARSET=latin1;


CREATE TABLE `testflight_users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `phone` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=44 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `titles_default_recurring_invites_data` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `title` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `default_recurring_invite_data_id` int NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `top_followed_users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `username` varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT '',
  `follower_count` int DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  KEY `username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `user_auth_tokens` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `device_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `auth_token` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `sns_arn` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `device_type` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `version` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `granted_notifs` tinyint NOT NULL DEFAULT '0',
  `device_model` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `uuid` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `total_failed_notifs` int NOT NULL DEFAULT '0',
  `last_used_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `os_version` varchar(30) COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `auth_token` (`auth_token`),
  UNIQUE KEY `uuid` (`uuid`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `user_calendars` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `email` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `google_calendar_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `google_access_token` text COLLATE utf8mb4_unicode_ci NOT NULL,
  `google_server_auth_code` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `google_sync_token` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `is_default` tinyint NOT NULL DEFAULT '0',
  `is_syncing` tinyint NOT NULL DEFAULT '0',
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_enabled` tinyint NOT NULL DEFAULT '1',
  `google_account_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `needs_re_authorization` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `email_google_calendar_id` (`email`,`google_calendar_id`),
  KEY `user_id` (`user_id`),
  KEY `google_account_id` (`google_account_id`),
  KEY `google_calendar_id_index` (`google_calendar_id`)
) ENGINE=InnoDB AUTO_INCREMENT=140 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `user_conversions` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `converter_id` int DEFAULT NULL,
  `user_id` int DEFAULT NULL,
  `signup_date` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;

CREATE TABLE `user_mentions` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `chater_id` int DEFAULT NULL,
  `mentioned_id` int DEFAULT NULL,
  `invite_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `group_id` int DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_composite_index` (`mentioned_id`,`chater_id`,`invite_id`)
) ENGINE=InnoDB AUTO_INCREMENT=238 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `user_ranks` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `user_rank` int DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;

CREATE TABLE `user_snstopics` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `subscription_arn` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `name` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `subscription_arn` (`subscription_arn`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=14 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `role_id` bigint DEFAULT NULL,
  `first` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `username` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso_code` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phone` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `picture` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `code` varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `signed_up` tinyint(1) DEFAULT '0',
  `key` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `granted_notifs` tinyint(1) DEFAULT '0',
  `granted_contacts` tinyint(1) DEFAULT '0',
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `timezone` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_session` datetime DEFAULT NULL,
  `location_hidden` tinyint(1) DEFAULT '0',
  `is_deleted` tinyint(1) DEFAULT '0',
  `birthday` date DEFAULT NULL,
  `gender` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_type` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_id` int DEFAULT NULL,
  `grade` int DEFAULT NULL,
  `signup_date` datetime DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_verified` tinyint NOT NULL DEFAULT '0',
  `bio` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_public` tinyint NOT NULL DEFAULT '0',
  `organization_id` int DEFAULT NULL,
  `ip_address` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_older_than_thirteen` tinyint NOT NULL DEFAULT '0',
  `profile_background` text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `app_download_token` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_name` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `api_content_provider_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_login` timestamp NULL DEFAULT NULL,
  `permissions` json DEFAULT NULL,
  `encrypted_email` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `encrypted_phone` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `password` varchar(60) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `opted_in_for_sms_to_contacts` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `phone` (`phone`),
  UNIQUE KEY `users_username_unique` (`username`),
  UNIQUE KEY `users_email_unique` (`email`),
  UNIQUE KEY `app_download_token` (`app_download_token`) USING BTREE,
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `api_content_provider_name_id_composite_index` (`api_content_provider_name`,`api_content_provider_id`),
  KEY `last_session` (`last_session`),
  KEY `signup_date` (`signup_date`),
  KEY `timezone` (`timezone`),
  KEY `picture` (`picture`),
  KEY `updated_at` (`updated_at`)
) ENGINE=InnoDB AUTO_INCREMENT=15000 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_copy` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `first` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `username` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso_code` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phone` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `picture` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `code` varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `auth_token` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `signed_up` tinyint(1) DEFAULT '0',
  `version` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `device_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `key` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sns_arn` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `granted_notifs` tinyint(1) DEFAULT '0',
  `granted_contacts` tinyint(1) DEFAULT '0',
  `device_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `device_model` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `last_location` datetime DEFAULT NULL,
  `timezone` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_session` datetime DEFAULT NULL,
  `location_hidden` tinyint(1) DEFAULT '0',
  `is_deleted` tinyint(1) DEFAULT '0',
  `birthday` date DEFAULT NULL,
  `gender` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_type` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_id` int DEFAULT NULL,
  `grade` int DEFAULT NULL,
  `app_available` tinyint(1) DEFAULT NULL,
  `signup_date` datetime DEFAULT NULL,
  `shown_review_prompt` tinyint(1) DEFAULT '0',
  `wants_in_asap` tinyint(1) DEFAULT '0',
  `image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `profile_background` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `feed_background` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `signup_state` varchar(5) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `last_seen_review_prompt` datetime DEFAULT NULL,
  `did_like_app` tinyint NOT NULL DEFAULT '0',
  `last_seen_twitter_prompt` datetime DEFAULT NULL,
  `did_tweet` tinyint NOT NULL DEFAULT '0',
  `last_seen_update_prompt` datetime DEFAULT NULL,
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_verified` tinyint NOT NULL DEFAULT '0',
  `bio` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_public` tinyint NOT NULL DEFAULT '0',
  `meetup_id` int DEFAULT NULL,
  `ticketmaster_id` varchar(155) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `eventbrite_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `edu_id` int NOT NULL,
  `ip_address` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `themoviedb_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_older_than_thirteen` tinyint NOT NULL DEFAULT '0',
  `web-blob` text COLLATE utf8mb4_unicode_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `phone` (`phone`),
  UNIQUE KEY `meetup_id` (`meetup_id`),
  UNIQUE KEY `users_ticketmaster_id_unique` (`ticketmaster_id`),
  UNIQUE KEY `users_eventbrite_id_unique` (`eventbrite_id`),
  UNIQUE KEY `users_username_unique` (`username`),
  UNIQUE KEY `users_email_unique` (`email`),
  UNIQUE KEY `users_themoviedb_id_unique` (`themoviedb_id`),
  KEY `first` (`first`),
  KEY `last` (`last`),
  KEY `last_session` (`last_session`),
  KEY `signup_date` (`signup_date`),
  KEY `school_id` (`school_id`),
  FULLTEXT KEY `first_full` (`first`),
  FULLTEXT KEY `last_full` (`last`),
  FULLTEXT KEY `username_full` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=1500 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_explore_sections` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `explore_section_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_explore_section_id_unique_index` (`user_id`,`explore_section_id`),
  KEY `users_explore_sections_user_id_index` (`user_id`),
  KEY `users_explore_sections_explore_section_id_index` (`explore_section_id`)
) ENGINE=InnoDB AUTO_INCREMENT=17 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_friends` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `friend_id` int DEFAULT NULL,
  `muted` tinyint(1) DEFAULT '0',
  `is_deleted` tinyint(1) DEFAULT '0',
  `ignored` tinyint(1) DEFAULT '0',
  `blocked` tinyint(1) DEFAULT '0',
  `is_close_friend` tinyint(1) DEFAULT '0',
  `reminder_notif_sent_at` datetime DEFAULT NULL,
  `friend_request_sent_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `friend_dismissed_friend_prompt` tinyint NOT NULL DEFAULT '0',
  `requested` tinyint(1) NOT NULL DEFAULT '0',
  `request_denied` tinyint NOT NULL DEFAULT '0',
  `is_subscribed` tinyint NOT NULL DEFAULT '0',
  `seen_close_friend` tinyint(1) DEFAULT '0',
  `is_followed_from_contacts` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `friend_id_user_id_index` (`friend_id`,`user_id`),
  KEY `user_id` (`user_id`),
  KEY `updated_at` (`updated_at`)
) ENGINE=InnoDB AUTO_INCREMENT=7989 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_friends_joined` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `joined_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_joined_id_index` (`user_id`,`joined_id`)
) ENGINE=InnoDB AUTO_INCREMENT=91 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_gestures` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `sender_id` int DEFAULT NULL,
  `receiver_id` int DEFAULT NULL,
  `gesture_id` int DEFAULT NULL,
  `is_reply` tinyint(1) DEFAULT '0',
  `reply_gesture_id` int DEFAULT NULL,
  `is_dismissed` tinyint(1) DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `receiver_gesture_index` (`receiver_id`,`sender_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_groups` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int unsigned NOT NULL,
  `group_id` int unsigned NOT NULL,
  `inviter_id` int unsigned NOT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NULL DEFAULT NULL,
  `is_seen` tinyint(1) NOT NULL DEFAULT '0',
  `chat_tab_updated_at` datetime DEFAULT NULL,
  `pending` tinyint NOT NULL DEFAULT '0',
  `joined_at` datetime DEFAULT NULL,
  `last_opened_at` datetime DEFAULT NULL,
  `mute_until` datetime DEFAULT NULL,
  `denied_at` timestamp NULL DEFAULT NULL,
  `is_requested` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_group_id_index` (`user_id`,`group_id`),
  KEY `users_groups_joined_at_index` (`joined_at`),
  KEY `inviter_id_index` (`inviter_id`),
  KEY `group_id_user_id` (`group_id`,`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=295 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_hashtags` (
  `user_id` int NOT NULL,
  `hashtag_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  UNIQUE KEY `user_id_hashtag_id_composite_index` (`user_id`,`hashtag_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_interests` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `interest_id` int NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `score` int NOT NULL,
  `did_like` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=39 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_nominations` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int DEFAULT NULL,
  `nomination_id` int DEFAULT NULL,
  `nominator_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `seen` tinyint(1) DEFAULT '0',
  `message` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `requested` tinyint(1) NOT NULL DEFAULT '0',
  `bolded_string` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `removed` tinyint(1) NOT NULL DEFAULT '0',
  `revealed` tinyint NOT NULL DEFAULT '0',
  `ignored_reveal_request` tinyint NOT NULL DEFAULT '0',
  `seen_reveal` tinyint NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_nomination_nominator` (`user_id`,`nomination_id`,`nominator_id`),
  KEY `created_at` (`created_at`),
  KEY `nominator_id` (`nominator_id`)
) ENGINE=InnoDB AUTO_INCREMENT=509 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_not_needed` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `first` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `username` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `iso_code` varchar(2) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `phone` varchar(20) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `email` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `picture` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `code` varchar(5) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `auth_token` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `signed_up` tinyint(1) DEFAULT '0',
  `version` varchar(10) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `device_token` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `key` varchar(64) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `sns_arn` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `granted_notifs` tinyint(1) DEFAULT '0',
  `granted_contacts` tinyint(1) DEFAULT '0',
  `device_type` varchar(20) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `device_model` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `latitude` double(18,12) unsigned DEFAULT NULL,
  `longitude` double(18,12) DEFAULT NULL,
  `last_location` datetime DEFAULT NULL,
  `timezone` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_session` datetime DEFAULT NULL,
  `location_hidden` tinyint(1) DEFAULT '0',
  `is_deleted` tinyint(1) DEFAULT '0',
  `birthday` date DEFAULT NULL,
  `gender` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_type` char(1) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `school_id` int DEFAULT NULL,
  `grade` int DEFAULT NULL,
  `app_available` tinyint(1) DEFAULT NULL,
  `signup_date` datetime DEFAULT NULL,
  `shown_review_prompt` tinyint(1) DEFAULT '0',
  `wants_in_asap` tinyint(1) DEFAULT '0',
  `image` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `profile_background` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `feed_background` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `signup_state` varchar(5) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `last_seen_review_prompt` datetime DEFAULT NULL,
  `did_like_app` tinyint NOT NULL DEFAULT '0',
  `last_seen_twitter_prompt` datetime DEFAULT NULL,
  `did_tweet` tinyint NOT NULL DEFAULT '0',
  `last_seen_update_prompt` datetime DEFAULT NULL,
  `did_suggest_nom` tinyint DEFAULT '0',
  `hex_color` varchar(16) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_verified` tinyint NOT NULL DEFAULT '0',
  `is_eventful_venue` tinyint NOT NULL DEFAULT '0',
  `is_eventful_performer` tinyint NOT NULL DEFAULT '0',
  `eventful_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `bio` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_public` tinyint NOT NULL DEFAULT '0',
  `meetup_id` int DEFAULT NULL,
  `ticketmaster_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `eventbrite_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `phone` (`phone`),
  UNIQUE KEY `key` (`key`),
  UNIQUE KEY `sns_arn` (`sns_arn`),
  UNIQUE KEY `eventful_id_index` (`eventful_id`),
  UNIQUE KEY `username` (`username`),
  UNIQUE KEY `auth_token` (`auth_token`),
  UNIQUE KEY `meetup_id` (`meetup_id`),
  UNIQUE KEY `users_ticketmaster_id_unique` (`ticketmaster_id`),
  UNIQUE KEY `users_eventbrite_id_unique` (`eventbrite_id`),
  KEY `active_phones` (`phone`,`signed_up`),
  KEY `first` (`first`),
  KEY `last` (`last`),
  KEY `is_active` (`signed_up`),
  KEY `timezone` (`timezone`),
  KEY `last_session` (`last_session`),
  KEY `is_deleted` (`is_deleted`),
  KEY `signup_date` (`signup_date`),
  KEY `school_id` (`school_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_searches` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `model` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `value` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_model_value_composite_index` (`user_id`,`model`,`value`)
) ENGINE=InnoDB AUTO_INCREMENT=1054 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_tapped` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `model` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `model_id` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `total` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_model_composite_index` (`user_id`,`model`,`model_id`)
) ENGINE=InnoDB AUTO_INCREMENT=413 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `users_tokens` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `token` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `name` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `token_unique_index` (`token`),
  UNIQUE KEY `user_id_name_token_unique_index` (`user_id`,`name`),
  UNIQUE KEY `users_tokens_user_id_unique` (`user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE `verified_users` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `user_id` int NOT NULL,
  `themoviedb_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `ticketmaster_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `eventbrite_id` varchar(155) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `is_sports_team` tinyint NOT NULL DEFAULT '0',
  `claimed` tinyint NOT NULL DEFAULT '0',
  `promote_account` tinyint NOT NULL DEFAULT '0',
  `promote_events` tinyint NOT NULL DEFAULT '0',
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id` (`user_id`),
  UNIQUE KEY `themoviedb_id` (`themoviedb_id`),
  UNIQUE KEY `ticketmaster_id` (`ticketmaster_id`),
  UNIQUE KEY `eventbrite_id` (`eventbrite_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
