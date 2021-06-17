class Employee3 < ApplicationRecord
  has_many :subordinates, class_name: "Employee3", foreign_key: "manager_id"
  belongs_to :manager, class_name: "Employee3", optional: true
end

