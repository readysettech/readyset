class Supplier5 < ApplicationRecord
  has_many :books, class_name: :Book6
  has_many :authors, class_name: :Author5, through: :books
end

