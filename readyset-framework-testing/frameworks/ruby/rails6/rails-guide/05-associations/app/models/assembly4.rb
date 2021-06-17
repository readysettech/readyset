class Assembly4 < ApplicationRecord
  has_and_belongs_to_many :parts,
    class_name: "Part4"
end

