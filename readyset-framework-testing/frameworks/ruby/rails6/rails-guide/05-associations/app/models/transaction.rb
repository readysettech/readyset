class Transaction < ApplicationRecord
  belongs_to :account, -> { includes :supplier, readonly: true },
    class_name: "Account5",
    foreign_key: "account5_id"
end

