# frozen_string_literal: true
# This migration comes from spree (originally 20180416083007)

class AddApplyToAllToVariantPropertyRule < ActiveRecord::Migration[5.1]
  def change
    add_column :spree_variant_property_rules, :apply_to_all, :boolean, default: false, null: false
    change_column :spree_variant_property_rules, :apply_to_all, :boolean, default: true
  end

  def down
    remove_column :spree_variant_property_rules, :apply_to_all
  end
end
