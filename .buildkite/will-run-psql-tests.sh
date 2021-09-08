#!/bin/bash -ex
[ "$(buildkite-agent meta-data get 'changed-frameworks' | jq '.postgres | length')" -gt 0 ]

