#!/usr/bin/env bats

load _helpers

@test "consul/server/bootstrapexpect: bootstrapExpect is 3" {
  cd `chart_dir`
  local actual=$(helm template \
      -s charts/consul/templates/server-config-configmap.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "ConfigMap") | select(.metadata.name == "release-name-consul-server-config")' | yq -r '.data."server.json"' | jq -r '.bootstrap_expect' | tee /dev/stderr)
  [ "${actual}" == "3" ]
}

