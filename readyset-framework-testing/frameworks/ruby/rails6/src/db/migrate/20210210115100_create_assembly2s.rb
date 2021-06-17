class CreateAssembly2s < ActiveRecord::Migration[6.1]
  def change
    create_table :assembly2s do |t|
      t.timestamps
    end

    create_table :part2s do |t|
      t.timestamps
    end

    create_table :assembly2s_part2s, id: false do |t|
      t.belongs_to :assembly2
      t.belongs_to :part2
    end
  end
end

