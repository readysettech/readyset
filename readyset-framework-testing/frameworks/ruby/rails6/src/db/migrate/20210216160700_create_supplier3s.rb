class CreateSupplier3s < ActiveRecord::Migration[6.1]
  def change
    create_table :supplier3s, id: false do |t|
      t.primary_key :guid
      t.string :name
      t.timestamps
    end

    create_table :account4s do |t|
      t.belongs_to :supplier3, index: true
      t.string :account_number
      t.timestamps
    end
  end
end

