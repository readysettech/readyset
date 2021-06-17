class CreateAssembly3s < ActiveRecord::Migration[6.1]
  def change
    create_table :assembly3s do |t|
      t.timestamps
    end

    create_table :part3s do |t|
      t.timestamps
    end

    create_table :manifests do |t|
      t.belongs_to :assembly3
      t.belongs_to :part3
    end
  end
end

