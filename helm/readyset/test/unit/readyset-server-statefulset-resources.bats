#!/usr/bin/env bats

load _helpers

@test "readyset/server/statefulset: default resources.requests.memory is 4294967296 bytes (4Gi)" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .resources.requests.memory' | tee /dev/stderr)
  [ "${actual}" == "4294967296" ]
}

@test "readyset/server/statefulset: default resources.limits.memory is 4294967296 bytes (4Gi)" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .resources.limits.memory' | tee /dev/stderr)
  [ "${actual}" == "4294967296" ]
}

@test "readyset/server/statefulset: default resources.requests.cpu is 1000m" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .resources.requests.cpu' | tee /dev/stderr)
  [ "${actual}" == "1000m" ]
}

@test "readyset/server/statefulset: custom resources.requests.cpu is 3.5" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.server.resources.requests.cpu=3.5' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .resources.requests.cpu' | tee /dev/stderr)
  [ "${actual}" == "3.5" ]
}

