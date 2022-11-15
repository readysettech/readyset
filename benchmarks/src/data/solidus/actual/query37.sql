SELECT `spree_roles`.* FROM `spree_roles` INNER JOIN `spree_roles_users` ON `spree_roles`.`id` = `spree_roles_users`.`role_id` WHERE `spree_roles_users`.`user_id` = 2;
