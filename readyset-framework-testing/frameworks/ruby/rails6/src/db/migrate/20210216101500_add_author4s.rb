class AddAuthor4s < ActiveRecord::Migration[6.1]
  def change
    create_table :author4s do |t|
      t.string :name
      t.boolean :active
      t.integer :num_books
      t.timestamps
    end

    create_table :book5s do |t|
      t.belongs_to :author4, foreign_key: true
      t.boolean :confirmed, default: false
      t.string :title
      t.timestamps
    end

    create_table :chapters do |t|
      t.belongs_to :book5, foreign_key: true
      t.integer :number
      t.string :name
      t.timestamps
    end
  end
end

