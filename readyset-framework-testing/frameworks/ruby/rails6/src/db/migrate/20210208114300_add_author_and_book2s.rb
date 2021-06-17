class AddAuthorAndBook2s < ActiveRecord::Migration[6.1]
  def change
    create_table :authors do |t|
      t.string :name
      t.timestamps
    end

    create_table :book2s do |t|
      t.belongs_to :author, index: true, foreign_key: true
      t.string :title
      t.datetime :published_at
      t.timestamps
    end
  end
end

