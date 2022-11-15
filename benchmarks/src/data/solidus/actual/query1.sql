SELECT `spree_stores`.* FROM `spree_stores` WHERE (`spree_stores`.`url` = 'localhost' OR `spree_stores`.`default` = TRUE) ORDER BY `spree_stores`.`default` ASC LIMIT 1;
