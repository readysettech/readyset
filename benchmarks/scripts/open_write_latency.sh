#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$(cargo locate-project --message-format plain)")"
results_dir="$(pwd)/benchmarks/results/"

mkdir -p "$results_dir"

indices=(0 1 5 10 50)

for num_indices in "${indices[@]}"; do
    file="$results_dir/results_${num_indices}.csv"
    if [ -f "$file" ]; then
        if [ -z "${FORCE+x}" ]; then
            echo "$file already exists and FORCE not set, refusing to overwrite file"
            exit 1
        else
            rm -f "$file"
        fi
    fi
done

cargo build --release --bin benchmarks
for num_indices in "${indices[@]}"; do
    echo "=== Benchmarking ${num_indices} indices ==="
    target/release/benchmarks \
        --local \
        --graph \
        --x-axis target-qps \
        --x-values 100,150,200,250,300,350,400,450,500,600,700,800,900,1000,1200,1400,1600,1800,2000,3000,4000,5000,6000,7000,8000,9000,10000 \
        --graph-results-path "${results_dir}/results_${num_indices}.csv" \
        write-benchmark \
        --schema benchmarks/src/data/single_table/db.sql \
        --run-for 120 \
        --threads 32 \
        --indices-per-table "$num_indices"
done

pushd benchmarks/scripts
gnuplot write_latency.gnuplot
popd

mv benchmarks/scripts/plot.png .

echo "Wrote plot.png"
