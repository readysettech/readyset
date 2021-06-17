class Chapter < ApplicationRecord
  belongs_to :book, -> { includes :author }, class_name: "Book5", foreign_key: "book5_id"
end

