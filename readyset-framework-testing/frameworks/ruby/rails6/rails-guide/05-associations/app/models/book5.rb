class Book5 < ApplicationRecord
  belongs_to :author, -> { where active: true }, class_name: "Author4", foreign_key: "author4_id", optional: true, inverse_of: :books, counter_cache: :num_books
  has_many :chapters
end

