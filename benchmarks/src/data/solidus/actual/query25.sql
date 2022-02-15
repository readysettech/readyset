SELECT `spree_taxons`.* FROM `spree_taxons` WHERE `spree_taxons`.`parent_id` IS NULL ORDER BY `spree_taxons`.`lft` ASC;
