class CreatePuppers < ActiveRecord::Migration[6.1]
  def change
    create_table :puppers do |t|
      t.string :name
      t.timestamps
    end

    create_table :puppers_friends, id: false do |t|
      t.references :this_pupper, index: true, foreign_key: { to_table: :puppers, on_delete: :cascade, on_update: :cascade }
      t.references :other_pupper, index: true, foreign_key: { to_table: :puppers, on_delete: :cascade, on_update: :cascade }
    end
  end
end

