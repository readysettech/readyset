class Account < ApplicationRecord
  validates :password, confirmation: true, unless: -> { password.blank? }
end

