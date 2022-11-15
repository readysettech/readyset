SELECT `spree_users`.* FROM `spree_users` WHERE `spree_users`.`deleted_at` IS NULL AND `spree_users`.`id` = ? ORDER BY `spree_users`.`id` ASC LIMIT 1;
