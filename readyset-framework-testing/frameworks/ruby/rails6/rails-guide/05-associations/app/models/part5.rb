class Part5 < ApplicationRecord
  has_and_belongs_to_many :manufacturers, -> { order("created_at DESC").limit(5).offset(2) }
  has_and_belongs_to_many :assemblies, class_name: :Assembly5
  has_and_belongs_to_many :assemblies_in_seattle, -> { where factory: "Seattle" },
    class_name: :Assembly5
end

