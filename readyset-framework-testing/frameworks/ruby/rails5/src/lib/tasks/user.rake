require 'terminal-table'

namespace :user do
  desc "Create user"
  task :create do
    User.create!(
      name:                  Faker::Name.name,
      email:                 Faker::Internet.email,
      password:              "password",
      password_confirmation: "password",
      activated:             true,
      activated_at:          Time.zone.now
    )
  end

  desc "List all users"
  task :list do
    users = User.all.collect { |user| [user.id, user.name, user.email, user.activated] }
    table = Terminal::Table.new :rows => users
    puts table
  end

  desc "Get a user by ID"
  task :get, [:id] do |t, args|
    user = User.find_by! id: args[:id]
    table = Terminal::Table.new :rows => [[user.id, user.name, user.email, user.activated, user.activated_at]]
    puts table
    feed = user.feed.collect { |mp| [mp.user, mp.id, mp.content] }
    table = Terminal::Table.new :rows => feed
    puts table
  end

  desc "Set a user's name"
  task :set_name, [:id, :name] do |t, args|
    user = User.find_by! id: args[:id]
    user.name = args[:name]
    user.save!
  end

  desc "Delete the first user"
  task :delete_first do
    users = User.all.first.destroy
  end

  desc "Create a user in a transaction, but bomb out of the transaction"
  task :create_fail do
    ActiveRecord::Base.transaction do
      user = User.create!(
        name:                  Faker::Name.name,
        email:                 Faker::Internet.email,
        password:              "password",
        password_confirmation: "password",
        activated:             true,
        activated_at:          Time.zone.now
      )
      raise StandardError, 'Nothing bad happened'
    end
  end
end

