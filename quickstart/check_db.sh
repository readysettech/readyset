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
default_port=5432
db_location=""

# Parse connection string (assuming format: "postgresql://user:password@host:port/dbname")
# Extract components from URL
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
                done
            fi
            attempts=0
            if [[ "$interactive_db_location" == "r" ]]; then
                while [[ -z $instance_id && $attempts -lt 3 ]]; do
                    ((attempts++))
                    # Prompt user for RDS Writer DB Identifier
                    read -rp "RDS Writer DB Identifier: " interactive_instance_id
                    # Validate interactive_instance_id is not empty
                    if [[ -z $interactive_instance_id ]]; then
                        echo "Invalid RDS Writer DB Identifier. Please enter a Identifier."
                        continue
                    fi
                    instance_id=$interactive_instance_id
                done
            fi
        fi
    else
        echo "AWS CLI is not installed. Please install AWS CLI to gather information necessary to configure Readyset Cloud."
    fi
    
}

function retrieve_ec2_instance_id_info() {
    local instance_id=$1
    data=$(aws ec2 describe-instances --instance-ids $instance_id --query 'Reservations[*].Instances[*].[PrivateDnsName,PrivateIpAddress,PublicDnsName,PublicIpAddress,VpcId,Placement.AvailabilityZone]' --output text)
    instance_private_dns=$(echo $data | awk '{print $1}')
    instance_private_ip=$(echo $data | awk '{print $2}')
    instance_public_dns=$(echo $data | awk '{print $3}')
    instance_public_ip=$(echo $data | awk '{print $4}')
    instance_vpc_id=$(echo $data | awk '{print $5}')
    instance_region=$(echo $data | awk '{print $6}' | awk '{print substr($1, 1, length($1)-1)}')
    instance_vpc_cidr_block=""
    if [[ -n $instance_vpc_id ]]; then
        instance_vpc_cidr_block=$(aws ec2 describe-vpcs --vpc-ids $instance_vpc_id --query 'Vpcs[*].[CidrBlock]' --output text)
    fi
}

function retrieve_rds_instance_id_info() {
    local instance_id=$1
    data=$(aws rds describe-db-instances --db-instance-identifier $instance_id --query 'DBInstances[*].[Endpoint.Address,AvailabilityZone,PubliclyAccessible,DBSubnetGroup.VpcId]' --output text)
    instance_endpoint_address=$(echo $data | awk '{print $1}')
    instance_region=$(echo $data | awk '{print $2}' | awk '{print substr($1, 1, length($1)-1)}')
    instance_public_available=$(echo $data | awk '{print $3}')
    instance_vpc_id=$(echo $data | awk '{print $4}')
    instance_vpc_cidr_block=""
    if [[ -n $instance_vpc_id ]]; then
        instance_vpc_cidr_block=$(aws ec2 describe-vpcs --vpc-ids $instance_vpc_id --query 'Vpcs[*].[CidrBlock]' --output text)
    fi
}

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



# Check if superuser
is_super=$(execute_sql "SELECT 
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
  "is_super": $is_super,
EOF
)

if [[ -n $instance_id ]]; then
    if [[ $db_location == "e" ]]; then
        retrieve_ec2_instance_id_info $instance_id
    output=$(cat <<EOF
$output
  "instance_id": $instance_id,
  "instance_private_dns": $instance_private_dns,
  "instance_private_ip": $instance_private_ip,
  "instance_public_dns": $instance_public_dns,
  "instance_public_ip": $instance_public_ip,
  "instance_vpc_id": $instance_vpc_id,
  "instance_region": $instance_region,
  "instance_vpc_cidr_block": $instance_vpc_cidr_block,
EOF
)
    elif [[ $db_location == "r" ]]; then
        retrieve_rds_instance_id_info $instance_id
    output=$(cat <<EOF
$output
  "instance_id": $instance_id,
  "instance_endpoint_address": $instance_endpoint_address,
  "instance_region": $instance_region,
  "instance_public_available": $instance_public_available,
  "instance_vpc_id": $instance_vpc_id,
  "instance_vpc_cidr_block": $instance_vpc_cidr_block,
EOF
)
    fi
fi

output=$(cat <<EOF
$output
}
EOF
)

echo ""
echo ""
echo ""
echo "Readyset Checklist Results:"
echo "$output"

# Exit script successfully
exit 0
