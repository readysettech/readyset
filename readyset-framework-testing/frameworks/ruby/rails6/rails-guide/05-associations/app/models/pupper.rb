class Pupper < ApplicationRecord
  has_and_belongs_to_many :friends,
    association_foreign_key: :other_pupper_id,
    autosave: true,
    class_name: :Pupper,
    foreign_key: :this_pupper_id,
    join_table: :puppers_friends,
    validate: true
end

