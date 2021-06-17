class Account2 < ApplicationRecord
  belongs_to :supplier
  has_one :account_history
end

