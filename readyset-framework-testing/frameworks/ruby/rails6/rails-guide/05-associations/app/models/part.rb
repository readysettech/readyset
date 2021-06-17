class Part < ApplicationRecord
  has_and_belongs_to_many :assemblies
end

