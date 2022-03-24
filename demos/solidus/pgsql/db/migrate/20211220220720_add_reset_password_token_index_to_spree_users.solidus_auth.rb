# frozen_string_literal: true
# This migration comes from solidus_auth (originally 20190125170630)

class AddResetPasswordTokenIndexToSpreeUsers < SolidusSupport::Migration[4.2]
  # We're not using the standard Rails index name because somebody could have
  #  already added that index to the table. By using a custom name we ensure
  # that the index can effectively be added and removed via migrations/rollbacks
  #  without having any impact on such installations. The index name is Rails
  # standard name + "_solidus_auth_devise"; the length is 61 chars which is
  # still OK for Sqlite, mySQL and Postgres.
  def custom_index_name
    'index_spree_users_on_reset_password_token_solidus_auth_devise'
  end

  def default_index_exists?
    index_exists?(:spree_users, :reset_password_token)
  end

  def custom_index_exists?
    index_exists?(:spree_users, :reset_password_token, name: custom_index_name)
  end

  def up
    Spree::User.reset_column_information
    if Spree::User.column_names.include?('reset_password_token') && !default_index_exists? && !custom_index_exists?
      add_index :spree_users, :reset_password_token, unique: true, name: custom_index_name
    end
  end

  def down
    if custom_index_exists?
      remove_index :spree_users, name: custom_index_name
    end
  end
end
