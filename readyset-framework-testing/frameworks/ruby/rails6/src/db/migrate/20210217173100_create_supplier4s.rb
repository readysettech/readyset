class CreateSupplier4s < ActiveRecord::Migration[6.1]
  def change
    create_table :supplier4s do |t|
      t.string :name
      t.timestamps
    end

    create_table :account5s do |t|
      t.belongs_to :supplier4, index: true
      t.string :account_number
      t.timestamps
    end

    create_table :transactions do |t|
      t.belongs_to :account5, foreign_key: true
      t.integer :value
      t.string :description
      t.timestamps
    end
  end
end

