#!/usr/bin/env bats

load _helpers

@test "readyset/server/statefulset: default value for env var READYSET_MEMORY_LIMIT is 4080218931 (95% of 4Gb)" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .env[] | select(.name == "READYSET_MEMORY_LIMIT") | .value' | tee /dev/stderr)
  [ "${actual}" == "4080218931" ]
}

@test "readyset/server/statefulset: default value for env var AUTHORITY_ADDRESS is readyset-consul-server:8500" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .env[] | select(.name == "AUTHORITY_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "readyset-consul-server:8500" ]
}

@test "readyset/server/statefulset: custom value for env var AUTHORITY_ADDRESS is consul.example.com:8500 when setting readyset.authority_address to consul.example.com" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'consul.enabled=false' \
      --set 'readyset.authority_address=consul.example.com' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .env[] | select(.name == "AUTHORITY_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "consul.example.com:8500" ]
}

@test "readyset/server/statefulset: custom value for env var AUTHORITY_ADDRESS is consul.example.com:8555 when setting readyset.authority_address to consul.example.com:8555" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'consul.enabled=false' \
      --set 'readyset.authority_address=consul.example.com:8555' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .env[] | select(.name == "AUTHORITY_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "consul.example.com:8555" ]
}

@test "readyset/server/statefulset: READYSET_MEMORY_LIMIT should be 4590246297 when readyset.server.resources.requests.memory=4.5Gi" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.server.resources.requests.memory=4.5Gi' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .env[] | select(.name == "READYSET_MEMORY_LIMIT") | .value' | tee /dev/stderr)
  [ "${actual}" == "4590246297" ]
}

@test "readyset/server/statefulset: READYSET_MEMORY_LIMIT should be 2375000000 when readyset.server.resources.limits.memory=2.5G (Overriding 4Gi requests default)" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-server-statefulset.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.server.resources.limits.memory=2.5G' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "StatefulSet") | select(.metadata.name == "readyset-server") | .spec.template.spec.containers[] | select(.name == "readyset-server") | .env[] | select(.name == "READYSET_MEMORY_LIMIT") | .value' | tee /dev/stderr)
  [ "${actual}" == "2375000000" ]
}
