# frozen_string_literal: true
# This migration comes from spree (originally 20210122110141)

class AddNameToSpreeAddresses < ActiveRecord::Migration[5.2]
  def up
    add_column :spree_addresses, :name, :string
    add_index :spree_addresses, :name
  end

  def down
    remove_index :spree_addresses, :name
    remove_column :spree_addresses, :name
  end
end
