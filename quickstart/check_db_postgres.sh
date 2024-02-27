#!/bin/bash


###################################################################################################
# Readyset Checklist Script
#
# Version: 2.0
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
# 8. Gather EC2 or RDS instance information
#
###################################################################################################


slow_query_time_ms=2000
default_port=5432
db_location=""

# Parse connection string (assuming format: "postgresql://user:password@host:port/dbname")
# This function will set the protocol, user, password, host, port, and dbname variables
parse_connection_string() {
  local connection_string=$1
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
  if [[ -z $dbname ]]; then
      echo "Database name is required for Readyset."
      exit 1
  fi
  # Check credentials
  execute_sql "SELECT 1" &> /dev/null || { echo "Failed to connect. Is this script being run somewhere that it can connect to the db? Check your connection string"; exit 1; }
}

# Provide prompts 
interactive_mode_prompts() {
    connection_string=$1
    if [[ -n $connection_string ]]; then
        parse_connection_string $connection_string
    else
        # Connection String
        echo -e "Provide your PostgreSQL database connection string in the format postgres://user:password@host:port/db_name."
        read -rp "Connection String: " interactive_connection_string
        parse_connection_string $interactive_connection_string
    fi

    # Check aws cli is installed
    check_aws_cli_installed

    # Validate its output
    if [[ $? -eq 0 ]]; then
        # Check if AWS CLI is configured
        check_aws_cli_configured
        if [[ $? -eq 1 ]]; then
            echo "AWS CLI is not configured. Please configure it via (aws configure or aws configure sso ) to gather information necessary to configure Readyset Cloud."
        else
            attempts=0
            while [[ -z $db_location && $attempts -lt 3 ]]; do
                ((attempts++))
                # Prompt user for database location
                echo "Are you running the database on EC2 or RDS?"
                read -rp "EC2(e)/RDS(r): " interactive_db_location

                # Validate user input
                if [[ "$interactive_db_location" != "e" && "$interactive_db_location" != "r" ]]; then
                    echo "Invalid input. Please enter either 'e' or 'r'."
                    continue
                fi
                db_location=$interactive_db_location
            done
            if [[ -z $db_location ]]; then
                exit 1
            fi
            attempts=0
            if [[ "$interactive_db_location" == "e" ]]; then
                list_ec2_instances
                while [[ -z $instance_id && $attempts -lt 3 ]]; do
                    ((attempts++))
                    # Prompt user for EC2 instance ID
                    read -rp "EC2 Instance ID (Eg: i-1234567890abcdef0): " interactive_instance_id
                    # Validate interactive_instance_id starts with "i-" followed by 17 characters ranging from numbers to hexadecimal
                    if [[ ! $interactive_instance_id =~ ^i-[0-9a-fA-F]{17}$ ]]; then
                        echo "Invalid EC2 Instance ID. Please enter a valid ID."
                        continue
                    fi
                    instance_id=$interactive_instance_id
                    retrieve_ec2_instance_id_info $instance_id
                done
            fi
            attempts=0
            if [[ "$interactive_db_location" == "r" ]]; then
                list_rds_instances
                while [[ -z $instance_id && $attempts -lt 3 ]]; do
                    ((attempts++))
                    # Prompt user for RDS Writer DB Identifier
                    read -rp "Enter your RDS Instance: " interactive_instance_id
                    # Validate interactive_instance_id is not empty
                    if [[ -z $interactive_instance_id ]]; then
                        echo "Invalid RDS Writer DB Identifier. Please enter a Identifier."
                        continue
                    fi
                    instance_id=$interactive_instance_id
                    retrieve_rds_instance_id_info $instance_id
                done
            fi
            attempts=0
            while [[ -z $aws_account_id && $attempts -lt 3 ]]; do
                ((attempts++))
                retrieve_aws_account_id
            done
        fi
    else
        echo "AWS CLI is not installed. Please install AWS CLI to gather information necessary to configure Readyset Cloud."
    fi
    
}

# Function to list EC2 instances
function list_ec2_instances() {
    aws ec2 describe-instances --query 'Reservations[*].Instances[*].[InstanceId,Tags[?Key==`Name`].Value[]]' --output text | paste - -
}

# Function to list RDS instances
function list_rds_instances() {
    local data=$(aws rds describe-db-instances --query 'DBInstances[*].[DBInstanceIdentifier]' --output text)
    if [[ ! -z $data ]]; then
        echo "RDS Instances:"
        for instance in $data; do
          echo $instance
        done
    fi
}

# Function to retrieve vpc CIDR block
function retrieve_vpc_cidr_block() {
    local vpc_id=$1
    instance_vpc_cidr_block=$(aws ec2 describe-vpcs --vpc-ids $vpc_id --query 'Vpcs[*].[CidrBlock]' --output text)
}

# Function to retrieve EC2 instance information
# This function will set the instance_vpc_id, instance_region, instance_public_ip, and instance_vpc_cidr_block variables
function retrieve_ec2_instance_id_info() {
    local instance_id=$1

    local data=$(aws ec2 describe-instances --instance-ids $instance_id --query 'Reservations[*].Instances[*].[VpcId,Placement.AvailabilityZone,PublicIpAddress]' --output text)
    instance_vpc_id=$(echo -e $data |awk '{print $1}')
    instance_region=$(echo -e $data |awk '{print $2}' | awk '{print substr($1, 1, length($1)-1)}')
    instance_public_ip=$(echo -e $data |awk '{print $3}')
    instance_vpc_cidr_block=""
    if [[ -n $instance_vpc_id ]]; then
        retrieve_vpc_cidr_block $instance_vpc_id
    fi
}

# Function to retrieve RDS instance information
# This function will set the instance_vpc_id, instance_region, instance_public_ip, and instance_vpc_cidr_block variables
function retrieve_rds_instance_id_info() {
    local instance_id=$1
    local data=$(aws rds describe-db-instances --db-instance-identifier $instance_id --query 'DBInstances[*].[DBSubnetGroup.VpcId,AvailabilityZone,PubliclyAccessible]' --output text)
    instance_vpc_id=$(echo $data |awk '{print $1}')
    instance_region=$(echo $data |awk '{print $2}' | awk '{print substr($1, 1, length($1)-1)}')
    instance_public_ip=$(echo $data |awk '{print $3}')
    instance_vpc_cidr_block=""
    if [[ -n $instance_vpc_id ]]; then
        retrieve_vpc_cidr_block $instance_vpc_id
    fi
}

# Function to retrieve AWS Account ID
# This function will set the aws_account_id variable
retrieve_aws_account_id() {
    aws_account_id=$(aws sts get-caller-identity --query 'Account' --output text)
}

# Function to execute SQL command and print result
execute_sql() {
    local sql=$1
    PGPASSWORD=${password} psql --host=${host} --port=${port} --username=${user} --dbname=${dbname} -t --quiet -c "${sql}" 
}

# Function that suggest instance size
suggest_rs_instance_size() {
    local size_in_bytes=$1
    local table_count=$2
    if [[ $size_in_bytes -lt "4831838208" ]] && [[ $table_count -lt 10 ]] ; then
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

# Naive Function that suggest CIDR block for VPC
suggest_vpc_cidr_block() {
    local vpc_cidr_block=$1
    local suggested_cidr_blocks=("172.31.0.0/16" "10.0.0.0/16" "192.168.0.0/20")
    for suggested_cidr_block in ${suggested_cidr_blocks[@]}; do
        if [[ $vpc_cidr_block != $suggested_cidr_block ]]; then
            echo "$suggested_cidr_block"
            break
        fi
    done
}

# Function to check if PostgreSQL instalation is compliant with Readyset
check_postgres_compliance() {
    local pg_version=$(execute_sql "SHOW server_version_num;")
    out_of_compliance=false
    if [[ $pg_version -lt "130000" ]]; then
        echo "Your PostgreSQL version is not supported. Please upgrade to PostgreSQL 13 or later."
        out_of_compliance=true
    fi

    # Check WAL level
    local wal_level=$(execute_sql "SHOW wal_level;")
    if [[ $wal_level != "logical" ]]; then
        echo "Your PostgreSQL WAL level is not supported. Please set wal_level to logical."
        out_of_compliance=true
    fi

    # Check if superuser
    local is_super=$(execute_sql "SELECT 
    CASE
        WHEN EXISTS (
            SELECT 1 
            FROM pg_roles 
            WHERE rolname = CURRENT_USER AND rolsuper
        ) THEN true
        WHEN EXISTS (
            SELECT 1
            FROM pg_roles, pg_auth_members 
            WHERE pg_roles.oid = pg_auth_members.roleid 
            AND pg_roles.rolname = 'rds_superuser' 
            AND pg_auth_members.member = (
                SELECT oid 
                FROM pg_roles 
                WHERE rolname = CURRENT_USER
            )
        ) THEN true
        ELSE false
    END;")
    if [[ $is_super != "t" ]]; then
        echo "Your user is not a superuser. Please use a superuser to run Readyset."
        out_of_compliance=true
    fi
    if [[ $out_of_compliance == true ]]; then
        echo "For more information, please visit https://docs.readyset.io/reference/configure-your-database/postgres"
        echo ""
        echo "Error: Your PostgreSQL installation is not compliant with Readyset. Please fix the issues above before connecting Readyset to this instance."
        exit 1
    fi
}

# Function responsible for checking psql is installed
check_psql_exists() {
  if ! command -v psql &>/dev/null; then
    echo -e "psql (PostgreSQL client) is not installed. Please install psql to continue."
    exit 1
  fi
}

# Function to check if AWS CLI is installed
check_aws_cli_installed() {
    if command -v aws &>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to check if AWS CLI is configured
check_aws_cli_configured() {
    if aws sts get-caller-identity &>/dev/null; then
        return 0
    else
        return 1
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
    interactive_mode_prompts $1
fi



# Check DB size
db_size=$(execute_sql "SELECT pg_database_size(current_database());")

# Check table count
table_count=$(execute_sql "SELECT COUNT(*)
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

# Suggest Instance Size
instace_size=$(suggest_rs_instance_size ${db_size} ${table_count})

# Check slow queries
slow_queries=$(execute_sql "SELECT CONCAT('You currently have ', COUNT(*), ' SELECT queries taking more than 2 seconds to run.' ) FROM pg_stat_statements WHERE LOWER(query) LIKE 'select%' AND mean_exec_time > ${slow_query_time_ms};" 2> /dev/null || echo "Error. Is pg_stat_statements extention installed? You can install it by running: 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements;' or check https://docs.readyset.io/reference/profiling-queries for instructions on how to install it and re-run the script.")


# Create JSON output
output=$(cat <<EOF
{
  "cache_type":  $instace_size,
  "user": $user,
  "host": $host,
  "port": $port,
  "db_name": $dbname,
EOF
)

if [[ -n $instance_id ]]; then       
  if [[ $instance_public_ip != "None" ]] && [[ $instance_public_ip != "False" ]]; then
    output=$(cat <<EOF
$output
  "connection_type": "public",
EOF
)
  else 

    output=$(cat <<EOF
$output
  "connection_type": "private",
  "upstream_aws_account_id": $aws_account_id,
  "upstream_vpc_region": $instance_region,
  "upstream_vpc_id": $instance_vpc_id,
  "upstream_vpc_cidr_block": $instance_vpc_cidr_block,
  "desired_cluster_cidr_block": $(suggest_vpc_cidr_block $instance_vpc_cidr_block),
EOF
)
  fi
fi

output=$(cat <<EOF
$output
}
EOF
)



########### Output ###########
echo ""
echo ""
echo ""
clear
echo "Readyset Create New Cache Fields:"
echo "Use below information to fill up your new Readyset Cloud Cache Instance:"

echo "$output"

echo ""
echo ""
echo "Slow Query Info:"
echo $slow_queries
echo ""
echo ""

check_postgres_compliance

# Exit script successfully
exit 0