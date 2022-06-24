#!/bin/bash
set -euo pipefail

ecr_repository_namespace="305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror"
images=(
    "consul:latest"
    "zookeeper:latest"

    "mysql:latest"
    "mysql:5.7"
    "mysql:8.0"
    "postgres:latest"
    "postgres:13"

    "redis:6.2"

    "rust:latest"
    "rust:1.54"
    "rust:1.55"
    "rustlang/rust:nightly"

    "node:15.12.0"
    "node:alpine"
    "golang:latest"
    "haskell:8.6"
    "openjdk:12"
    "openjdk:alpine"
    "drupal:9-apache"
    "php:7-apache"
    "python:3"
    "python:3.9-alpine3.12"
    "python:3.9-alpine3.14"
    "ruby:2.6.5-alpine"
    "ruby:2.6.3"

    "debian:bullseye-slim"
    "debian:stretch-slim"
    "alpine:latest"
    "fedora:32"
    "ubuntu:20.04"
)

echo "steps:"

for image in "${images[@]}"; do
cat << EOF
  - name: ":docker: Mirror $image image"
    commands:
      - docker pull $image
      - docker tag $image $ecr_repository_namespace/$image
      - docker push $ecr_repository_namespace/$image
    plugins:
      ecr#v2.5.0:
        login: true
        retries: 3

  - wait
EOF
done
