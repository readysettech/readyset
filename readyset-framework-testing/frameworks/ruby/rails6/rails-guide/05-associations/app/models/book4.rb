class Book4 < ApplicationRecord
  belongs_to :author,
    autosave: true,
    class_name: "Author3",
    counter_cache: true,
    # dependent covered in "main" (non-guide) Rails 6 test
    foreign_key: "author3_id",
    primary_key: "guid",
    inverse_of: :book4s,
    # polymorphic covered in section 2.9, see employee.rb, product.rb, and picture.rb
    touch: true,
    validate: true,
    optional: true
end

