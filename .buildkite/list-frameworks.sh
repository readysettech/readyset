#!/bin/bash -ex

dir="$(dirname "${0}")"
pip3 install pyyaml
export OUTPUT=plain
./readyset-framework-testing/tools/get-frameworks | buildkite-agent meta-data set 'frameworks'
changed_frameworks="$("${dir}/get-changed-files.sh" | ./readyset-framework-testing/tools/get-frameworks)"
if [ "${BUILDKITE_BRANCH}" == 'master' ] && [ "${NIGHTLY}" == 'true' ]; then
  # In the case of master on nightly, we just test every framework
  ./readyset-framework-testing/tools/get-frameworks | buildkite-agent meta-data set 'changed-frameworks'
elif [ "${changed_frameworks}" != '' ]; then
  export FILTER_FROM_STDIN=1
  echo "${changed_frameworks}" | buildkite-agent meta-data set 'changed-frameworks'
fi
