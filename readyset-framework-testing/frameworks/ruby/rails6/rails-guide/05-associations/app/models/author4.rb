class Author4 < ApplicationRecord
  has_many :books, -> { includes :chapters },
    # as covered earlier in section 2.9, see product.rb and employee.rb
    class_name: "Book5",
    counter_cache: :num_books,
    dependent: :nullify,
    foreign_key: :author4_id,
    inverse_of: :author,
    primary_key: :id,
    # source not covered because this is not a through association
    # source_type not covered because this is not a through association
    # through already covered in section 2.8, see assembly3.rb and part3.rb
    validate: true
  has_many :confirmed_books, -> { where confirmed: true },
    class_name: "Book5"
  has_many :chapters, -> { order('number asc').limit(100).offset(12) },
    through: :books
  has_many :grouped_chapters, -> { group 'book5s.id' },
    through: :books,
    source: :chapters
  has_many :chapter_numbers, -> { select('chapters.id, book5_id, number').distinct },
    through: :books,
    source: :chapters,
    class_name: 'ChapterNumber'
end

