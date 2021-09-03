#!/bin/bash -ex

dir="$(dirname "${0}")"
pip3 install pyyaml
export OUTPUT=plain
./readyset-framework-testing/tools/get-frameworks | buildkite-agent meta-data set 'frameworks'
changed_frameworks="$("${dir}/get-changed-files.sh" | FILTER_FROM_STDIN=1 ./readyset-framework-testing/tools/get-frameworks)"
if [ "${NIGHTLY}" == 'true' ]; then
  # In the case of main on nightly, we just test every framework
  ./readyset-framework-testing/tools/get-frameworks | buildkite-agent meta-data set 'changed-frameworks'
elif [ "${changed_frameworks}" != '' ]; then
  echo "${changed_frameworks}" | buildkite-agent meta-data set 'changed-frameworks'
fi
