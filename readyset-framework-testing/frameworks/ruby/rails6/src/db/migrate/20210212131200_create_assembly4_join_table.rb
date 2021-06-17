class CreateAssembly4JoinTable < ActiveRecord::Migration[6.1]
  def change
    create_join_table :assembly4s, :part4s do |t|
      t.index :assembly4_id
      t.index :part4_id
    end
  end
end

