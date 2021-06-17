class Author2 < ApplicationRecord
  has_many :book3s, inverse_of: 'writer'
end

