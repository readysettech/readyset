class CreateAssembly5s < ActiveRecord::Migration[6.1]
  def change
    create_table :assembly5s do |t|
      t.string :factory
      t.string :name
      t.timestamps
    end

    create_table :part5s do |t|
      t.string :kind
      t.string :sku
      t.timestamps
    end

    create_table :manufacturers do |t|
      t.string :name
      t.timestamps
    end

    create_join_table :assembly5s, :part5s do |t|
      t.index :assembly5_id
      t.index :part5_id
    end

    create_join_table :part5s, :manufacturers do |t|
      t.index :part5_id
      t.index :manufacturer_id
    end
  end
end

