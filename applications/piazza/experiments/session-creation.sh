#!/bin/bash

# Experiment: Session creation time
#
# The purpose of this experiment is to measure the impact of
# reuse and partial materialization on session creation time.

if [ $# -lt 3 ]
  then
    echo "usage: ./run.sh [dir] [policies] [queries]"
    exit
fi

mkdir $1
policies=$2
queries=$3

case "$OSTYPE" in
  darwin*)  cmd="gtime" ;;
  linux*)   cmd="/usr/bin/time" ;;
esac

declare -a setups=(
    "--reuse full"
    "--reuse noreuse"
    "--reuse full --partial"
    "--reuse noreuse --partial"
)

declare -a setup_names=(
    "full-nopartial"
    "noreuse-nopartial"
    "full-partial"
    "noreuse-partial"
)

nuser=5000

setupslength=${#setups[@]}

for (( i=1; i<${setupslength}+1; i++ ));
do
    setup=${setups[$i-1]}
    setup_name=${setup_names[$i-1]}
    name=$nuser-$setup_name
    mkdir $1/$name
    mkdir $1/$name/info

    $cmd -v cargo run --manifest-path benchmarks/Cargo.toml --bin=piazza --release -- \
        -l $nuser -u $nuser -i $1/$name/info/info -p 1000000 -c 1000 --populate before --private 0.2 $setup --policies $policies -q $queries > $1/$name/results-$name.out 2> $1/$name/results-$name.log

done

