#!/bin/bash
dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
url="https://people.csail.mit.edu/malte/projects/soup/hotcrp-testdata.tar.gz"

mkdir -p ${dir}/testdata

wget ${url} -O ${dir}/testdata/hotcrp-testdata.tar.gz
cd ${dir}/testdata

tar -xzf hotcrp-testdata.tar.gz
rm hotcrp-testdata.tar.gz

cd -
