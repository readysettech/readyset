class GoodnessValidator < ActiveModel::Validator
  def validate(record)
    if options[:fields].any? { |field| record.send(field).include? "evil" }
      record.errors.add :base, "This person is evil"
    end
  end
end

class Person3 < ApplicationRecord
  validates :email, confirmation: true, exclusion: { in: ["inva@l.id"] }, format: { with: /\A[a-z]+@[a-z]+.[a-z.]+\z/ }, length: { minimum: 6, maximum: 12 }, uniqueness: true
  validates :email_confirmation, presence: true
  validates_with GoodnessValidator, fields: [:email]
  validates_each :email do |record, attr, value|
    record.errors.add(attr, "must not contain capital letters") if value =~ /[[:upper:]]/
  end
end

