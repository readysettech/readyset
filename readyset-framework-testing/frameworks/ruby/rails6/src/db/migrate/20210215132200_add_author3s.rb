class AddAuthor3s < ActiveRecord::Migration[6.1]
  def change
    create_table :author3s, id: false do |t|
      t.primary_key :guid
      t.string :name
      t.integer :book4s_count
      t.timestamps
    end

    create_table :book4s do |t|
      t.string :title
      t.timestamps
    end

    add_column :book4s, :author3_id, :bigint, index: true
    add_foreign_key :book4s, :author3s, column: :author3_id, primary_key: :guid
  end
end

