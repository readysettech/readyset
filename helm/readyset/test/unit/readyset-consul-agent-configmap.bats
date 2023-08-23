#!/usr/bin/env bats

load _helpers

@test "readyset/consul/agent: default configmap should use k8s as auto-join provider" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-consul-agent-configmap.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "ConfigMap") | select(.metadata.name == "readyset-consul-agent-cm") | .data["entrypoint.sh"]' | sed -n 's/.*-retry-join="provider=\([^[:space:]]*\).*/\1/p' \
      | tee /dev/stderr)
  [ "${actual}" == "k8s" ]
}

@test "readyset/consul/agent: configmap should use direct retry-join when consul.enabled is false and readyset.authority_address is configured" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-consul-agent-configmap.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'consul.enabled=false' \
      --set 'readyset.authority_address=consul.example.com' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "ConfigMap") | select(.metadata.name == "readyset-consul-agent-cm") | .data["entrypoint.sh"]' | sed -n 's/.*-retry-join=\([^[:space:]]*\).*/\1/p' \
      | tee /dev/stderr)
  [ "${actual}" == "\${AUTHORITY_ADDRESS}" ]
}
