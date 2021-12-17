#!/bin/bash
# Packages the set of files and binaires required to test ReadySet on a
# deployment. Creates deployment_test.tar.gz.
#
# ./package-deployment-tests.sh <readyset_root_directory>
#  readyset_root_directory: The root directory of the monorepo.

# Must be run from the readyset root directory.
READYSET="./"
if [ "$#" -eq 1 ]; then
    READYSET=$1
fi

# Go to the root directory.
cd $READYSET

# Set of binaries that we want to be included in the testing package.
binaries=(benchmarks irl_minimal verify_prometheus_metrics basic_validation_test data_generator)

# Add each of the binaries above to the cargo build cmd. We use docker to build
# the binaries for our cloudformation deployment's os.
build_cmd="./build/ubuntu20.04/cargo-ubuntu2004 build --release"
for binary in "${binaries[@]}"; do
  build_cmd+=" --bin $binary"
done

# Run the build command. This creates binaries in ./target-ubuntu2004/release
$build_cmd

# Create a directory where we will copy relevant files to package.
TMP_DIR=tmp-package-deployment-tests
mkdir $TMP_DIR

# Binaries!
for binary in "${binaries[@]}"; do
  cp ./target-ubuntu2004/release/$binary $TMP_DIR
done

# Directory structure needs to match monorepo since .yaml files use the
# path in the monorepo.
DATA_DIR_PATH=$TMP_DIR/src/data/
mkdir -p $DATA_DIR_PATH
cp -r benchmarks/src/data/* $DATA_DIR_PATH
cp benchmarks/src/yaml/benchmarks/* $TMP_DIR
cp benchmarks/src/yaml/deployments/example.yaml $TMP_DIR/deployment.example.yaml

tar -cvf deployment_test.tar.gz -C $TMP_DIR .
rm -r $TMP_DIR
