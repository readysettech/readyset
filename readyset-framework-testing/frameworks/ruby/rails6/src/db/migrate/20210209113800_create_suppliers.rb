class CreateSuppliers < ActiveRecord::Migration[6.1]
  def change
    create_table :suppliers do |t|
      t.string :name
      t.timestamps
    end

    create_table :account2s do |t|
      t.belongs_to :supplier, index: { unique: true }, foreign_key: true
      t.string :account_number
      t.timestamps
    end

    create_table :account_histories do |t|
      t.belongs_to :account2
      t.integer :credit_rating
      t.timestamps
    end
  end
end

