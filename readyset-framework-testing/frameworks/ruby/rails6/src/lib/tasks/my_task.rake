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

namespace :db do
  desc "Do a sql dump. Equivalent to setting `config.active_record.schema_format=:sql` in application.rb and calling `rake db:schema:dump`"
  task :sqldump do
    ActiveRecord::Base.schema_format = :sql
    Rake::Task["db:schema:dump"].invoke
  end
end