class Topic < ApplicationRecord
  validates :title, length: { is: 5 }, allow_blank: true
end

