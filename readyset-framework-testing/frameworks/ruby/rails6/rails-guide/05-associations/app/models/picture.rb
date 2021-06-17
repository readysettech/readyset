class Picture < ApplicationRecord
  belongs_to :imageable, polymorphic: true
end

