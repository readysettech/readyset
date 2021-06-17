class Order < ApplicationRecord
  validates :card_number, presence: true, if: :paid_with_card?
  def paid_with_card?
    payment_type == "card"
  end
end

