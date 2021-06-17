class CreateEmployee3s < ActiveRecord::Migration[6.1]
  def change
    create_table :employee3s do |t|
      t.integer :manager_id
      t.string :name
      t.timestamps
    end
  end
end

