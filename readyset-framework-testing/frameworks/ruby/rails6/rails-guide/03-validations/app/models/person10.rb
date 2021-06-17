class Person10 < ApplicationRecord
  validate do |person|
    errors.add :base, :invalid, message: "This person is invalid because ..."
  end
end

