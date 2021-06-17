class Author5 < ApplicationRecord
  has_many :books, -> { order(year_published: :desc) },
    class_name: :Book6
end

