SELECT 1 AS one FROM `spree_variants` WHERE `spree_variants`.`deleted_at` IS NULL AND `spree_variants`.`product_id` = 1 AND `spree_variants`.`is_master` = FALSE LIMIT 1;
