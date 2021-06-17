class AddInvoiceAndCustomer < ActiveRecord::Migration[6.1]
  def change
    create_table :invoices do |t|
      t.integer :total_value
      t.integer :discount
      t.datetime :expiration_date
      t.timestamps
    end

    create_table :customers do |t|
      t.belongs_to :invoice
      t.string :name
      t.boolean :active
      t.timestamps
    end
  end
end

