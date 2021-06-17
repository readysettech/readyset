class Book3 < ApplicationRecord
  belongs_to :writer, class_name: 'Author2', foreign_key: 'author2_id'
end

