class Part3 < ApplicationRecord
  has_many :manifests
  has_many :assembly3s, through: :manifests
end

