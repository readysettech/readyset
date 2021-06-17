class Computer < ApplicationRecord
  has_one :market
  has_one :trackpad
  validates :mouse, presence: true, if: -> { market.retail? }, unless: -> { trackpad.present? }
end

