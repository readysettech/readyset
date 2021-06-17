class Review < ApplicationRecord
  belongs_to :customer, class_name: :Customer2, foreign_key: :customer2_id
  belongs_to :book, class_name: :Book6, foreign_key: :book6_id

  enum state: [:not_reviewed, :published, :hidden]
end

