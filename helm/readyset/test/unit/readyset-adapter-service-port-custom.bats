#!/usr/bin/env bats

load _helpers

@test "readyset/adapter/service: custom service port should be 5433" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-service.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.adapter.port=5433' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Service") | select(.metadata.name == "readyset-adapter") | .spec.ports[] | select((.name == "postgresql") or (.name == "mysql")) | .port' | tee /dev/stderr)
  [ "${actual}" == "5433" ]
}

@test "readyset/adapter/service: default service type is LoadBalancer" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-service.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Service") | select(.metadata.name == "readyset-adapter") | .spec.type' | tee /dev/stderr)
  [ "${actual}" == "LoadBalancer" ]
}

@test "readyset/adapter/service: custom service type is ClusterIP" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-service.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.adapter.service.type=ClusterIP' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Service") | select(.metadata.name == "readyset-adapter") | .spec.type' | tee /dev/stderr)
  [ "${actual}" == "ClusterIP" ]
}

