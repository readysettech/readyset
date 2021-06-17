class CreateSupplier2s < ActiveRecord::Migration[6.1]
  def change
    create_table :supplier2s do |t|
      t.string :name
      t.timestamps
    end

    create_table :account3s do |t|
      t.bigint :supplier2_id
      t.string :account_number
      t.timestamps
    end

    add_index :account3s, :supplier2_id
  end
end

