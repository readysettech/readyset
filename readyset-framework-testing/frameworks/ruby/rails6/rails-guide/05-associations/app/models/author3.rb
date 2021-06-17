class Author3 < ApplicationRecord
  self.primary_key = 'guid'
  has_many :book4s
end

