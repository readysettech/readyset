#!/bin/bash
## assumes you are running from root directory of readyset/noria-client/examples

echo "Pulling docker conainers"
sudo docker run -d -it --rm --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper:1.3
sudo docker run -d -it --rm --name kafka -p 9092:9092 --link zookeeper:zookeeper debezium/kafka:1.3
sudo docker run -d -it --rm --name mysql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw debezium/example-mysql:1.3
sudo docker run -d -it --rm --name connect -p 8083:8083 -e GROUP_ID=1 -e CONFIG_STORAGE_TOPIC=my_connect_configs -e OFFSET_STORAGE_TOPIC=my_connect_offsets -e STATUS_STORAGE_TOPIC=my_connect_statuses --link zookeeper:zookeeper --link kafka:kafka --link mysql:mysql debezium/connect:1.3

echo "waiting for mysql db"
sleep 10

echo "Initializing fresh MySQL DB"
mysql -h 127.0.0.1 --port 3306 -u "root" "-pdebezium" < "ryw-init.sql"

echo "Starting noria deployment=ryw"
cd ../../noria
cargo run --bin noria-server -- --deployment ryw --no-reuse --address 127.0.0.1 --shards 0