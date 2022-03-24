# frozen_string_literal: true
# This migration comes from spree (originally 20200320144521)
class AddDefaultBillngFlagToUserAddresses < ActiveRecord::Migration[5.2]
  def change
    add_column :spree_user_addresses, :default_billing, :boolean, default: false
  end
end
