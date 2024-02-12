#!/bin/bash


###################################################################################################
# Readyset Checklist Script
#
# Version: 1.0
#
# This Script Will:
#
# 1. Attempt to connect to your database and validate credentials
# 2. Check if user is super user
# 3. Gather current database size
# 4. Gather number of tables in current database
# 5. Calculate suggested Readyset Database Size
# 6. Check wal_level
# 7. Get the number of slow queries (if pg_stat_statement is enabled)
#
###################################################################################################


default_slow_query_ms=2000

# Usage check
if [ "$#" -ne 2 ] && [ "$#" -ne 1 ]; then
  echo "Usage: $0 <connection_string> [slow_query_time_in_ms]"
  echo "example: $0 postgresql://user:password@host:port/dbname 1000"
  exit 1
fi

connection_string=$1
slow_query_time_ms=${2:-2000}
default_port=5432


# Parse connection string (assuming format: "postgresql://user:password@host:port/dbname")
# Extract components from URL
protocol=$(echo $connection_string | grep -o '^[^:]*')
user=$(echo $connection_string | cut -d '/' -f 3 | cut -d ':' -f 1)
password=$(echo $connection_string | cut -d ':' -f 3 | cut -d '@' -f 1)
host=$(echo $connection_string | cut -d '@' -f 2 | cut -d ':' -f 1)
port=$(echo $connection_string | cut -d ':' -f 4 | cut -d '/' -f 1 || echo "${default_port}")
dbname=$(echo $connection_string | cut -d '/' -f 4)

# Function to execute SQL command and print result
execute_sql() {
    local sql=$1
    PGPASSWORD=${password} psql --host=${host} --port=${port} --username=${user} --dbname=${dbname} -t --quiet -c "${sql}" 
}

# Function that suggest instance size
suggest_rs_instance_size() {
    local size_in_bytes=$1
    if [[ $size_in_bytes -lt "4831838208" ]]; then
        echo "Free"
    elif [[ $size_in_bytes -lt "48318382080" ]]; then
        echo "Small"
    elif [[ $size_in_bytes -lt "209379655680" ]]; then
        echo "Medium"
    elif [[ $size_in_bytes -lt "531502202880" ]]; then
        echo "Large"
    else
        echo "Your database is larger than our cloud platform currently supports, but we are happy to work with you if you reach out to info@readyset.io."
    fi
}

check_psql_exists() {
  if ! command -v psql &>/dev/null; then
    echo -e "psql (PostgreSQL client) is not installed. Please install psql to continue."
    exit 1
  fi
}

# Check psql is installed
check_psql_exists

# Check credentials
execute_sql "SELECT 1" &> /dev/null || { echo "Failed to connect"; exit 1; } 


# Check if superuser
is_super=$(execute_sql "SHOW is_superuser;")

# Check DB size
db_size=$(execute_sql "SELECT pg_database_size(current_database());")

# Check table count
table_count=$(execute_sql "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_database();")

# Suggest Instance Size
instace_size=$(suggest_rs_instance_size ${db_size})

# Check WAL level
wal_level=$(execute_sql "SHOW wal_level;")

# Check slow queries
slow_queries=$(execute_sql "SELECT COUNT(*) FROM pg_stat_statements WHERE mean_exec_time > ${slow_query_time_ms};" 2> /dev/null || echo "Error. Is pg_stat_statements extention installed? Check https://docs.readyset.io/reference/profiling-queries for instructions on how to install it and re-run the script.")


# Create JSON output
output=$(cat <<EOF
{
  "host": $host,
  "database_size": $db_size,
  "database_size_gb": $((db_size / 1024 / 1024 / 1024)),
  "readyset_instance_size":  $instace_size
  "table_count": $table_count,
  "wal_level": $wal_level,
  "slow_queries": $slow_queries,
  "is_super": $is_super
}
EOF
)

echo "$output"

# Exit script successfully
exit 0
