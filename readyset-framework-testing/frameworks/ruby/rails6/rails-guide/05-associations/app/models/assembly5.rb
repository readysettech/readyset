class Assembly5 < ApplicationRecord
  has_and_belongs_to_many :parts, -> { includes "manufacturers" },
    class_name: :Part5
  has_many :manufacturers, -> { group("part5s.id").distinct.order("manufacturers.id ASC") },
    through: :parts
end

