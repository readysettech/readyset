#!/bin/sh

${SETUP_DATA_VOLUME}

cat > /etc/zookeeper/conf/zoo.cfg <<EOF
# https://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_configuration
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/var/lib/zookeeper
clientPort=2181
%{ for index, server in ZOOKEEPER_IPS ~}
server.${index + 1}=${server}:2888:3888
%{ endfor ~}
EOF

systemctl stop zookeeper
setup_data_volume /var/lib/zookeeper
echo ${ZOOKEEPER_ID} > /var/lib/zookeeper/myid
chown -R zookeeper:zookeeper /var/lib/zookeeper
systemctl reset-failed
systemctl restart zookeeper
