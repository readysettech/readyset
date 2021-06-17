class CreateEmployee2s < ActiveRecord::Migration[6.1]
  def change
    create_table :employee2s do |t|
      t.references :manager, foreign_key: { to_table: :employee2s }
      t.string :name
      t.timestamps
    end
  end
end

