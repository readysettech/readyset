class Account5 < ApplicationRecord
  belongs_to :supplier,
    class_name: "Supplier4",
    foreign_key: "supplier4_id"
  has_many :transactions
end

