#!/bin/bash

# Experiment: Session creation time
#
# The purpose of this experiment is to measure the impact of
# reuse and partial materialization on session creation time.

if [ $# -lt 2 ]
  then
    echo "usage: ./run.sh [dir] [dbname]"
    exit
fi

dbname=$2

mkdir $1

case "$OSTYPE" in
  darwin*)  cmd="gtime" ;;
  linux*)   cmd="/usr/bin/time" ;;
esac

nuser=1000
interval=100

for j in {0..10}
do
    nlogged=$((j * interval))
    name=$nlogged-mysql
    mkdir $1/$name
    mkdir $1/$name/info

    $cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=security-mysql --release -- \
        $dbname -l $nlogged -u $nuser -p 1000000 -c 1000 --private 0.2 > $1/$name/results-$name.out
        # 2> $1/$name/results-$name.log

done

