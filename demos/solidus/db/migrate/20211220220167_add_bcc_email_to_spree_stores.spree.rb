# frozen_string_literal: true
# This migration comes from spree (originally 20200530111458)

class AddBccEmailToSpreeStores < ActiveRecord::Migration[5.2]
  def change
    add_column :spree_stores, :bcc_email, :string
  end
end
