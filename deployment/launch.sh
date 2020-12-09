#!/usr/bin/env bash

function suppress { /bin/rm --force /tmp/suppress.out 2> /dev/null; ${1+"$@"} > /tmp/suppress.out 2>&1 || cat /tmp/suppress.out; /bin/rm /tmp/suppress.out; }


function get_sudo() {
    if ! sudo -S true < /dev/null 2> /dev/null; then
        echo "sudo access required for docker:"
        sudo true
    fi
}

get_sudo
suppress sudo docker run --rm --net=host --name zookeeper -d zookeeper:3.6
sleep 1
suppress sudo docker run --rm --net=host --name noria-server -d readyset-deploy:latest noria/target/release/noria-server  --deployment myapp --no-reuse --address 127.0.0.1 --shards 0
sleep 1
suppress sudo docker run --rm --net=host --name noria-mysql -d readyset-deploy:latest noria-mysql/target/release/noria-mysql --deployment myapp
suppress sudo docker run --rm --net=host --name noria-ui -d readyset-deploy:latest bash -lc 'conda activate readyset; cd noria-ui; ./run.sh'

echo 'Running! C-c to exit'
( trap exit SIGINT ; read -r -d '' _ </dev/tty )
trap '' SIGINT

get_sudo
suppress sudo docker kill zookeeper
suppress sudo docker kill noria-server
suppress sudo docker kill noria-mysql
suppress sudo docker kill noria-ui

