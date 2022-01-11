require 'active_record/connection_adapters/mysql2_adapter'

ActiveRecord::ConnectionAdapters::Mysql2Adapter.class_eval do
  def begin_db_transaction
  end

  def commit_db_transaction
  end

  def create_savepoint
  end

  def rollback_to_savepoint
  end

  def release_savepoint
  end
end
