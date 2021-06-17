class Manufacturer < ApplicationRecord
  has_and_belongs_to_many :parts, class_name: :Part5
end

