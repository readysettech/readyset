class CreateAssembly4s < ActiveRecord::Migration[6.1]
  def change
    create_table :assembly4s do |t|
      t.string :name
      t.timestamps
    end

    create_table :part4s do |t|
      t.string :part_number
      t.timestamps
    end
  end
end

