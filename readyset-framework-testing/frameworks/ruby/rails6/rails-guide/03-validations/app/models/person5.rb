class Person5 < ApplicationRecord
  validates :name, presence: { message: "must be given please" }
  validates :age, numericality: { message: "%{value} seems wrong" }
  validates :username, uniqueness: {
    message: ->(object, data) do
      "Hey #{object.name}, #{data[:value]} is already taken."
    end
  }
end

