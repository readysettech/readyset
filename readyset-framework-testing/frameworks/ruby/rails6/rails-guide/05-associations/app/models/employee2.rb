class Employee2 < ApplicationRecord
  has_many :subordinates, class_name: "Employee2", foreign_key: "manager_id"
  belongs_to :manager, class_name: "Employee2", optional: true
end

