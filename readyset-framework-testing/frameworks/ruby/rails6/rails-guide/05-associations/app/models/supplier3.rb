class Supplier3 < ApplicationRecord
  self.primary_key = 'guid'
  has_one :account,
    # as covered earlier in section 2.9, see product.rb and employee.rb
    autosave: true,
    class_name: "Account4",
    dependent: :nullify,
    foreign_key: :id,
    inverse_of: :supplier,
    primary_key: "guid",
    required: false,
    # source not covered because this is not a through association
    # source_type not covered because this is not a through association
    # through already covered in section 2.8, see assembly3.rb and part3.rb
    touch: true,
    validate: true
end

