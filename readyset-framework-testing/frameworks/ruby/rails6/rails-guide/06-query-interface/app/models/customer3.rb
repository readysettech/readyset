class Customer3 < ApplicationRecord
  self.table_name = :customer2s
  has_many :orders, class_name: :Order2, inverse_of: :customer, counter_cache: :orders_count
  has_many :reviews

  validates :first_name, presence: true
  validates :orders_count, presence: true
end


