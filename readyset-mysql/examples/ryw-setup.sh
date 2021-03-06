#!/bin/bash
## assumes you are running from root directory of readyset/readyset-client/examples

echo "Pulling docker conainers"
sudo docker run -d -it --rm --name zookeeper -p 2181:2181 debezium/zookeeper:1.5
sudo docker run -d -it --rm --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.5

echo "waiting for mysql db"
# Note that sometimes the mysql DB needs a long time to start up.
# If there is an error, be sure to manually enter the mysql command below to make sure DB is setup properly.
sleep 15

echo "Initializing fresh MySQL DB"
mysql -h 127.0.0.1 --port 3306 -u "root" "-pdebezium" < "ryw-init.sql"

echo "Starting ReadySet deployment=ryw"
cd ../../readyset
cargo run --bin readyset-server -- --deployment ryw --no-reuse --address 127.0.0.1 --shards 0 --upstream-db-url mysql://root:debezium@localhost/inventory
