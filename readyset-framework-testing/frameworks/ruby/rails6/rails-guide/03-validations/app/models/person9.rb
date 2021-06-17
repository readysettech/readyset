class Person9 < ApplicationRecord
  validate do |person|
    errors.add :name, :too_plain, message: "is not cool enough"
  end
end

