#!/bin/bash

dir="$(dirname "${0}")"

# shellcheck disable=SC2034
# QUIET is used in run_test
export QUIET=1;
for framework in $("${dir}/run.sh" get_frameworks); do
  "${dir}/run.sh" run_test "${framework}"
done

