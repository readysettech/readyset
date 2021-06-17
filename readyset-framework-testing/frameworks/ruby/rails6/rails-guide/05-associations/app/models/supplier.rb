class Supplier < ApplicationRecord
  has_one :account2
  has_one :account_history, through: :account2
end

