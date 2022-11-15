SELECT `spree_taxons`.* FROM `spree_taxons` WHERE `spree_taxons`.`parent_id` IS NULL AND `spree_taxons`.`taxonomy_id` IN (1, 2);
