#!/usr/bin/env bats

load _helpers

@test "readyset/server/service: default service type is LoadBalancer" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-service.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Service") | select(.metadata.name == "readyset-server") | .spec.type' | tee /dev/stderr)
  [ "${actual}" == "LoadBalancer" ]
}

@test "readyset/server/service: custom service type is ClusterIP" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-service.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.server.service.type=ClusterIP' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Service") | select(.metadata.name == "readyset-server") | .spec.type' | tee /dev/stderr)
  [ "${actual}" == "ClusterIP" ]
}
