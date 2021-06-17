class Numbers < ApplicationRecord
  validates :integer, numericality: { only_integer: true }
  validates :float, numericality: true
  validates :string, absence: true
end

