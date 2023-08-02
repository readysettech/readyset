#!/usr/bin/env bats

load _helpers

@test "consul/server/replicas: replicas is 3" {
  cd `chart_dir`
  local actual=$(helm template \
      -s charts/consul/templates/server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "release-name-consul-server")' | yq -r '.spec.replicas' | tee /dev/stderr)
  [ "${actual}" == "3" ]
}

