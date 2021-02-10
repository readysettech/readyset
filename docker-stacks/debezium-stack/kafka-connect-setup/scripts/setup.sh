#!/usr/bin/env bash
/opt/scripts/wait-for-server.sh -t 60 kafkaconnect:8083

if [ $? -ne 0 ]
then
	echo "Failed to wait for Kafka Connect to be ready. Exiting..."
	exit 1
fi

echo "Server is up, waiting a bit more until it is ready"
sleep 5

for filename in connectors/*/*.json; do
    echo "Creating connectors for file $filename"
    curl -f -v -i -X POST -H "Accept:application/json" \
      -H "Content-Type:application/json" http://kafkaconnect:8083/connectors \
      -d @$filename
    if [ $? -ne 0 ]
    then
    	echo "Failed to add Kafka Connector. Filename: $filename. Exiting..."
    	exit 1
    fi
done