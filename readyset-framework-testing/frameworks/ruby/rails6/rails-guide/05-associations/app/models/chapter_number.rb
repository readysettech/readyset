class ChapterNumber < ApplicationRecord
  self.table_name = 'chapters'
  belongs_to :book, class_name: "Book5", foreign_key: "book5_id"
end

