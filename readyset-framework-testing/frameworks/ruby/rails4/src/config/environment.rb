require 'active_record/connection_adapters/mysql2_adapter'

# Set MySQL to clear sql mode for all connections
class ActiveRecord::ConnectionAdapters::Mysql2Adapter
  def supports_foreign_keys?
    false
  end

  def supports_views?
    false
  end

  def configure_connection
    variables = @config.fetch(:variables, {}).stringify_keys
    @connection.query_options[:as] = :array
    if @config[:encoding]
            encoding = +"NAMES #{@config[:encoding]}"
            encoding << " COLLATE #{@config[:collation]}" if @config[:collation]
    end
    # don't do anything to configure the connection
    execute "SET #{encoding}"#
    if sql_mode = variables.delete("sql_mode")
      sql_mode = quote(sql_mode)
      execute "SET @sql_mode=#{sql_mode}"#
    end
  end
  def create_database(name, options = {})
    # don't try to create databases
  end
  def recreate_database(name, options = {})
    # don't recreate databses either
  end
end

# Load the Rails application.
require_relative 'application'

# Initialize the Rails application.
Rails.application.initialize!
