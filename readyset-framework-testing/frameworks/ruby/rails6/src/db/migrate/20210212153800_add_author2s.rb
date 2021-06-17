class AddAuthor2s < ActiveRecord::Migration[6.1]
  def change
    create_table :author2s do |t|
      t.string :first_name
      t.timestamps
    end

    create_table :book3s do |t|
      t.belongs_to :author2, foreign_key: true
      t.string :title
      t.timestamps
    end
  end
end

