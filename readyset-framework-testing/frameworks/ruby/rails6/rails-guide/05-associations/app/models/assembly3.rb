class Assembly3 < ApplicationRecord
  has_many :manifests
  has_many :part3s, through: :manifests
end

