#!/bin/bash


###################################################################################################
# Readyset DB Size Script
#
# Version: 1.0
#
# This Script Will:
#
# 1. Attempt to connect to your database and validate credentials
# 2. Gather current database size
# 3. Gather number of tables in current database
#
###################################################################################################


default_port=

# Parse connection string (assuming format: "postgresql://user:password@host[:port]/dbname")
# Or (assuming format: "mysql://user:password@host[:port]/[dbname]")
# This function will set the protocol, user, password, host, port, and dbname variables
parse_connection_string() {
  local connection_string=$1
  local db_mode=$2
  protocol=$(echo $connection_string | grep -o '^[^:]*')
  user=$(echo $connection_string | cut -d '/' -f 3 | cut -d ':' -f 1)
  password=$(echo $connection_string | cut -d ':' -f 3 | cut -d '@' -f 1)
  # Check if user has the "@" signal - empty password
  if [[ $user == *"@"* ]]; then
      user=$(echo $user | cut -d '@' -f 1)
      password=""
  fi
  # Check if password is empty
  if [[ -z $password ]]; then
      echo "Password for user ${user}. Hit ENTER if the user has no password"
      read -sp "Password: " password
      echo ""
  fi
  
  host_port=$(echo $connection_string | cut -d '@' -f 2 | cut -d '/' -f 1)
  host=$(echo $host_port | cut -d ':' -f 1)
  port=$default_port
  if [[ $host_port == *":"* ]]; then
      port=$(echo $host_port | cut -d ':' -f 2)
  fi
  dbname=$(echo $connection_string | cut -d '/' -f 4)
  if [[ -z $dbname ]] && [[ "$db_mode" == "postgres" ]]; then
      echo "Database name is required for Readyset."
      exit 1
  fi
  # Check credentials
  if [[ $db_mode == "postgresql" ]]; then
      execute_sql_pg "SELECT 1" &> /dev/null || { echo "Failed to connect. Is this script being run somewhere that it can connect to the db? Check your connection string"; exit 1; }
  else
      execute_sql_mysql "SELECT 1" &> /dev/null || { echo "Failed to connect. Is this script being run somewhere that it can connect to the db? Check your connection string"; exit 1; }
  fi
  
}

# Provide prompts 
interactive_mode_prompts() {
    echo "Welcome to the Readyset Database Size Checker!"
    echo "Please enter your database connection details."
    echo "MySQL connection string format: mysql://user:password@host:port/dbname"
    echo "PostgreSQL connection string format: postgresql://user:password@host:port/dbname"

    read -p "Enter the database connection: " connection_string

    if [[ $connection_string == mysql* ]]; then
        default_port=3306
        parse_connection_string $connection_string "mysql"
        run_mysql_commands
    elif [[ $connection_string == postgres* ]]; then
        default_port=5432
        parse_connection_string $connection_string "postgresql"
        run_postgresql_commands
    else
        echo "Invalid database mode. Please enter either mysql or postgresql connection string."
        exit 1
    fi

}

# Function that runs the required commands for PostgreSQL
run_postgresql_commands() {
   # Check DB size
    db_size=$(execute_sql_pg "SELECT pg_database_size(current_database());")

    # Check table count
    table_count=$(execute_sql_pg "SELECT COUNT(*)
        FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n
        ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'v') AND n.nspname <> 'pg_catalog'
                                AND n.nspname <> 'information_schema'
                                AND n.nspname <> 'readyset'
                                AND n.nspname !~ '^pg_toast'
                                AND c.oid NOT IN(
        SELECT c.oid
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_attribute a
            ON a.attrelid = c.oid
            WHERE attgenerated <> ''
        );")

}

# Function that runs the required commands for MySQL
run_mysql_commands() {
    # Check DB size
    table_schema="WHERE table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')"
    if [[ ! -z $dbname ]]; then
        table_schema="${table_schema} AND table_schema = '${dbname}'"
    fi

    db_size=$(execute_sql_mysql "SELECT SUM(data_length + index_length) FROM information_schema.tables  ${table_schema};")

    # Check table count
    table_count=$(execute_sql_mysql "SELECT COUNT(*) FROM information_schema.tables ${table_schema};")
}

# Function to execute SQL command and print result - PostgreSQL
execute_sql_pg() {
    local sql=$1
    PGPASSWORD=${password} psql --host=${host} --port=${port} --username=${user} --dbname=${dbname} -t --quiet -c "${sql}" 
}


# Function responsible for checking psql is installed
check_psql_exists() {
  if ! command -v psql &>/dev/null; then
    echo -e "psql (PostgreSQL client) is not installed. Please install psql to continue."
    exit 1
  fi
}

# Function to execute SQL command and print result - MySQL
execute_sql_mysql() {
    local sql=$1
    if [[ ! -z $db_name ]]; then
        db="-D ${dbname}"
    fi
    mysql -h ${host} -P ${port} -u ${user} -p${password} ${db} -s -N -e "${sql}"
}

# function to check if mysql client is installed
check_mysql_exists() {
  if ! command -v mysql &>/dev/null; then
    echo -e "mysql (MySQL client) is not installed. Please install mysql to continue."
    exit 1
  fi
}

##### End of functions #####

# Main run steps

# Check psql is installed
check_psql_exists

# Usage check
if [ "$#" -ne 1 ] ; then
  interactive_mode_prompts
else
    connection_string=$1
    if [[ $connection_string == mysql* ]]; then
        default_port=3306
        parse_connection_string $connection_string "mysql"
        run_mysql_commands
    elif [[ $connection_string == postgres* ]]; then
        default_port=5432
        parse_connection_string $connection_string "postgresql"
        run_postgresql_commands
    else
        echo "Invalid connection string. Please provide a valid mysql or postgres connection string."
        exit 1
    fi
fi

# Create output
output=$(cat <<EOF
  "Instance Size":  $db_size,
  "Number of Tables": $table_count
EOF
)


########### Output ###########
echo ""
echo ""
echo ""
clear
echo "Readyset Instace Size Fields:"

echo "$output"

echo ""
echo ""

# Exit script successfully
exit 0