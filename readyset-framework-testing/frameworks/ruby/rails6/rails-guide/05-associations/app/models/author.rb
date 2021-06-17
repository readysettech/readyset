class Author < ApplicationRecord
  has_many :book2s, dependent: :destroy
end

