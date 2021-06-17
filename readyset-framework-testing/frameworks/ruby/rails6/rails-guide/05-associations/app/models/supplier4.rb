class Supplier4 < ApplicationRecord
  has_one :account, class_name: "Account5"
end

