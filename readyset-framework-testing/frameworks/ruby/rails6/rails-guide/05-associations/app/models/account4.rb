class Account4 < ApplicationRecord
  belongs_to :supplier,
    class_name: "Supplier3",
    foreign_key: "supplier3_id",
    optional: true
end

