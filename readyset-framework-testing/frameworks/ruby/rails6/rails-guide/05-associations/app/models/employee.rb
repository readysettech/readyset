class Employee < ApplicationRecord
  has_many :pictures, as: :imageable
end

