class Book6 < ApplicationRecord
  belongs_to :supplier, class_name: :Supplier5, foreign_key: :supplier5_id
  belongs_to :author, class_name: :Author5, foreign_key: :author5_id
  has_many :reviews
  has_and_belongs_to_many :orders, class_name: :Order2, join_table: :book6s_order2s

  scope :in_print, -> { where(out_of_print: false) }
  scope :out_of_print, -> { where(out_of_print: true) }
  scope :old, -> { where('year_published < ?', 50.years.ago) }
  scope :newish, -> { where('year_published >= ?', 50.years.ago) }
  scope :costs_more_than, ->(amount) { where('price > ?', amount) }
  scope :out_of_print_and_expensive, -> { out_of_print.costs_more_than(500) }
end

