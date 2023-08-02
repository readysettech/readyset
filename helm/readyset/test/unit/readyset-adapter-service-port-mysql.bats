#!/usr/bin/env bats

load _helpers

@test "readyset/adapter/service: default MYSQL service port should be 3306" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-service.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.adapter.type=mysql' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Service") | select(.metadata.name == "readyset-adapter") | .spec.ports[] | select((.name == "postgresql") or (.name == "mysql")) | .port' | tee /dev/stderr)
  [ "${actual}" == "3306" ]
}

