SELECT `spree_variants`.* FROM `spree_variants` WHERE `spree_variants`.`product_id` = ? AND `spree_variants`.`is_master` = TRUE LIMIT 1;
