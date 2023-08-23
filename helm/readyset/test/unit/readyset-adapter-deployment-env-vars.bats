#!/usr/bin/env bats

load _helpers

@test "readyset/adapter/deployment: default value for env var METRICS_ADDRESS is 0.0.0.0:6034" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .env[] | select(.name == "METRICS_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "0.0.0.0:6034" ]
}

@test "readyset/adapter/deployment: default value for env var LISTEN_ADDRESS is 0.0.0.0:5432" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .env[] | select(.name == "LISTEN_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "0.0.0.0:5432" ]
}

@test "readyset/adapter/deployment: default value for env var AUTHORITY_ADDRESS is readyset-consul-server:8500" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .env[] | select(.name == "AUTHORITY_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "readyset-consul-server:8500" ]
}

@test "readyset/adapter/deployment: custom value for env var AUTHORITY_ADDRESS is consul.example.com:8500 when setting readyset.authority_address to consul.example.com" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'consul.enabled=false' \
      --set 'readyset.authority_address=consul.example.com' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .env[] | select(.name == "AUTHORITY_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "consul.example.com:8500" ]
}

@test "readyset/adapter/deployment: custom value for env var AUTHORITY_ADDRESS is consul.example.com:8555 when setting readyset.authority_address to consul.example.com:8555" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'consul.enabled=false' \
      --set 'readyset.authority_address=consul.example.com:8555' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .env[] | select(.name == "AUTHORITY_ADDRESS") | .value' | tee /dev/stderr)
  [ "${actual}" == "consul.example.com:8555" ]
}
