#!/usr/bin/env bats

load _helpers

@test "consul/enabled: Consul Helm Chart is enabled by default" {
  cd `chart_dir`
  local actual=$(cat values.yaml | yq e '.consul.enabled' | tee /dev/stderr)
  [ "${actual}" == "true" ]
}

@test "consul/server/bootstrapexpect: bootstrapExpect is 3" {
  cd `chart_dir`
  local actual=$(helm template \
      -s charts/consul/templates/server-config-configmap.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "ConfigMap") | select(.metadata.name == "release-name-consul-server-config")' | yq -r '.data."server.json"' | jq -r '.bootstrap_expect' | tee /dev/stderr)
  [ "${actual}" == "3" ]
}

@test "consul/server/replicas: replicas is 3" {
  cd `chart_dir`
  local actual=$(helm template \
      -s charts/consul/templates/server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "release-name-consul-server")' | yq -r '.spec.replicas' | tee /dev/stderr)
  [ "${actual}" == "3" ]
}
