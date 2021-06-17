namespace :hello do
  desc "Hello task"
  task :greet do
    puts "Hello from JetBrains!"
  end

  desc "Goodbye task"
  task :bye do
    puts "Goodbye from JetBrains!"
  end
end