#!/usr/bin/env bats

load _helpers

@test "readyset/adapter/deployment: default resources.requests.memory is 2Gi" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .resources.requests.memory' | tee /dev/stderr)
  [ "${actual}" == "2Gi" ]
}

@test "readyset/adapter/deployment: default resources.limits.memory is 2Gi" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .resources.limits.memory' | tee /dev/stderr)
  [ "${actual}" == "2Gi" ]
}

@test "readyset/adapter/deployment: custom resources.limits.memory is 2.5Gi" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.adapter.resources.limits.memory=2.5Gi' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .resources.limits.memory' | tee /dev/stderr)
  [ "${actual}" == "2.5Gi" ]
}

@test "readyset/adapter/deployment: default resources.requests.cpu is 500m" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .resources.requests.cpu' | tee /dev/stderr)
  [ "${actual}" == "500m" ]
}

@test "readyset/adapter/deployment: custom resources.requests.cpu is 3.5" {
  cd `chart_dir`
  local actual=$(helm template \
      -s templates/readyset-adapter-deployment.yaml \
      --set 'readyset.deployment=readyset-helm-test' \
      --set 'readyset.adapter.resources.requests.cpu=3.5' \
      . | tee /dev/stderr |
      yq e '. | select(.kind == "Deployment") | select(.metadata.name == "readyset-adapter") | .spec.template.spec.containers[] | select(.name == "readyset-adapter") | .resources.requests.cpu' | tee /dev/stderr)
  [ "${actual}" == "3.5" ]
}

