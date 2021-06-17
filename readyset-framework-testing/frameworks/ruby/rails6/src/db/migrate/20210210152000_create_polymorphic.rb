class CreatePolymorphic < ActiveRecord::Migration[6.1]
  def change
    create_table :pictures do |t|
      t.string :name
      t.references :imageable, polymorphic: true
      t.timestamps
    end

    create_table :employees do |t|
      t.string :name
      t.timestamps
    end

    create_table :products do |t|
      t.string :name
      t.timestamps
    end
  end
end

