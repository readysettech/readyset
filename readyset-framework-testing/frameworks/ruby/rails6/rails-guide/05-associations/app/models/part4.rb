class Part4 < ApplicationRecord
  has_and_belongs_to_many :assemblies,
    class_name: "Assembly4"
end

