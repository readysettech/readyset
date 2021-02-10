#!/usr/bin/env bash
# Run PostgreSQL queries
for filename in queries/postgres/*.sql; do
	PGPASSWORD=debezium psql -h 127.0.0.1 -p 5432 -d test -U root < $filename
	if [ $? -ne 0 ]
	then
		echo "Failed to execute PostgreSQL query. Filename: $filename. Exiting..."
		exit 2
	fi
done

# Run MariaDB queries
for filename in queries/mariadb/*.sql; do
    mysql -h 127.0.0.1 --port 3306 --database test -u root --password=debezium < $filename
    if [ $? -ne 0 ]
    then
    	echo "Failed to execute MySQL query. Filename: $filename. Exiting..."
    	exit 3
    fi
done