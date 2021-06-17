class AddComputerMarketAndTrackpad < ActiveRecord::Migration[6.1]
  def change
    create_table :computers do |t|
      t.string :mouse
      t.timestamps
    end

    create_table :markets do |t|
      t.belongs_to :computer
      t.boolean :retail
      t.timestamps
    end

    create_table :trackpads do |t|
      t.belongs_to :computer
      t.boolean :present
      t.timestamps
    end
  end
end

