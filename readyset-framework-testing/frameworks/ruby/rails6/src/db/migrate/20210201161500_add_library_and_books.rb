class AddLibraryAndBooks < ActiveRecord::Migration[6.1]
  def change
    create_table :library do |t|
      t.string :name
      t.timestamps
    end

    create_table :books do |t|
      t.string :title
      t.timestamps
    end

    add_reference :books, :library
    add_foreign_key :books, :library
  end
end

