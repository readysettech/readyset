class Person4 < ApplicationRecord
  validates :opt_in, inclusion: { in: ["yes", "no"] }
end

