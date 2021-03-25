# TODO: configure to listen on the transaction metadata topic as well
echo "Install connector"
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors -d '{ "name": "inventory-connector", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1", "database.hostname": "mysql", "database.port": "3306", "database.user": "debezium", "database.password": "dbz", "database.server.id": "184054", "database.server.name": "dbserver1", "database.include.list": "inventory", "database.history.kafka.bootstrap.servers": "kafka:9092", "database.history.kafka.topic": "dbhistory.inventory", "key.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter": "org.apache.kafka.connect.json.JsonConverter", "decimal.handling.mode" : "double", "time.precision.mode": "connect", "provide.transaction.metadata": "true"} }'

cd ../../connectors
echo "Starting Connector"
cargo run --bin debezium_connector -- -t employees -k localhost:9092 -s dbserver1 -d inventory -z 127.0.0.1:2181 --deployment ryw