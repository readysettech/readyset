class Customer2 < ApplicationRecord
  has_many :orders, class_name: :Order2, inverse_of: :customer, counter_cache: :orders_count
  has_many :reviews
end

