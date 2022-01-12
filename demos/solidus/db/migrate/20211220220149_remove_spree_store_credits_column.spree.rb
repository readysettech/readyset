# frozen_string_literal: true
# This migration comes from spree (originally 20170223235001)

class RemoveSpreeStoreCreditsColumn < ActiveRecord::Migration[5.0]
  def change
    remove_column :spree_store_credits, :spree_store_credits, :datetime
  end
end
