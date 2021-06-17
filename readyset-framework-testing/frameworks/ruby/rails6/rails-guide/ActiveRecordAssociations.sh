#!/bin/sh -ex

source helper.sh

# 1 Why Associations?
rails db:migrate
rake guide_association:step_1

# 2 The Types of Associations
rake guide_association:step_2_1 # The belongs_to Association
rake guide_association:step_2_2 # The has_one Association
rake guide_association:step_2_3 # The has_many Association
rake guide_association:step_2_4 # The has_many :through Association
rake guide_association:step_2_5 # The has_one :through Association
rake guide_association:step_2_6 # The has_and_belongs_to_many Association
rake guide_association:step_2_7 # Choosing between belongs_to and has_one
rake guide_association:step_2_8 # Choosing between has_many :through and has_and_belongs_to_many
rake guide_association:step_2_9 # Polymorphic Associations
rake guide_association:step_2_10 # Self Joins

# 3 Tips, Tricks, and Warnings
rake guide_association:step_3_1 # Controlling Caching
# 3.2 Avoiding Name Collisions is just advice about naming fields
rake guide_association:step_3_3 # Updating the schema
# 3.4 Controlling Association Scope is only examples for more fine-grained Ruby module organization, nothing to do with the database
rake guide_association:step_3_5 # Bi-directional Associations

# 4 Detailed Association Reference
# 4.1 belongs_to Association Reference
rake guide_association:step_4_1_1 # Methods Added by belongs_to
rake guide_association:step_4_1_2 # Options for belongs_to
rake guide_association:step_4_1_3 # Scopes for belongs_to
rake guide_association:step_4_1_4 # Do Any Associated Objects Exist?
# 4.1.5 When are Objects Saved? is just answering a question
# 4.2 has_one Association Reference
rake guide_association:step_4_2_1 # Methods Added by has_one
rake guide_association:step_4_2_2 # Options for has_one
rake guide_association:step_4_2_3 # Scopes for has_one
rake guide_association:step_4_2_4 # Do Any Associated Objects Exist?
# 4.2.5 When are Objects Saved? is just answering a question
# 4.3 has_many Association Reference
rake guide_association:step_4_3_1 # Methods Added by has_many
# 4.3.2 Options for has_many is well-covered in the 4.3.1 test
rake guide_association:step_4_3_3 # Scopes for has_many
# 4.3.4 When are Objects Saved? is just answering a question
# 4.4 has_and_belongs_to_many Association Reference
rake guide_association:step_4_4_1 # Methods Added by has_and_belongs_to_many
# 4.4.2 Options for has_and_belongs_to_many is well-covered in the 4.4.1 test
rake guide_association:step_4_4_3 # Scopes for has_and_belongs_to_many
# 4.4.4 When are Objects Saved? is just answering a question
# 4.5 Association Callbacks doesn't explain any database-facing functionality
# 4.6 Association Extensions doesn't explain any database-facing functionality

# 5 Single Table Inheritance (STI)
rails generate model vehicle type:string color:string price:decimal{10.2}
rails generate model car --parent=vehicle
rails generate model motorcycle --parent=vehicle
rails generate model bicycle --parent=vehicle
rails db:migrate
rake guide_association:step_5

