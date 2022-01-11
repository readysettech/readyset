# frozen_string_literal: true
# This migration comes from spree (originally 20161129035810)

class AddIndexToSpreePaymentsNumber < ActiveRecord::Migration[5.0]
  def change
    add_index 'spree_payments', ['number'], unique: true
  end
end
