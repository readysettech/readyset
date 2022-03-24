# frozen_string_literal: true
# This migration comes from spree (originally 20210312061050)

class ChangeColumnNullOnPrices < ActiveRecord::Migration[5.2]
  def change
    change_column_null(:spree_prices, :amount, false)
  end
end
