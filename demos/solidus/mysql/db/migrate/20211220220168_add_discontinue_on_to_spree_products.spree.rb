# frozen_string_literal: true
# This migration comes from spree (originally 20201008213609)

class AddDiscontinueOnToSpreeProducts < ActiveRecord::Migration[5.2]
  def change
    add_column :spree_products, :discontinue_on, :datetime
  end
end
