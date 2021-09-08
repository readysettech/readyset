#!/usr/bin/env python3

import json
import os
import subprocess
import sys

dialect_names = {"mysql": "MySQL", "postgres": "Postgres"}
dialect_versions = {"mysql": ["mysql80"], "postgres": ["postgres13"]}

compose_template = """
version: '3.8'
services:
  test:
    image: "305232526136.dkr.ecr.us-east-2.amazonaws.com/frameworks/${DIALECT}/${FRAMEWORK}"
    depends_on:
      db:
        condition: service_healthy
    links:
      - db
"""

build_template = """
  - key: build-${DIALECT}-${FRAMEWORK_SLUG}
    label: "Build test container image for ${FRAMEWORK} on ${DIALECT}"
    command: ./.buildkite/build-framework-image.sh '${DIALECT}' '${FRAMEWORK}'
    plugins:
      - ecr#v2.2.0:
          login: true
"""


def generate_build_step(framework, dialect):
    framework_slug = framework.replace("/", "_").replace(":", "_")
    step = (
        build_template.replace("${FRAMEWORK}", framework)
        .replace("${FRAMEWORK_SLUG}", framework_slug)
        .replace("${DIALECT}", dialect)
    )
    return step


test_template = """
  - key: test-${SERVICE}-${FRAMEWORK_SLUG}
    label: "Test ${FRAMEWORK} against ${SERVER} (${DIALECT_VERSION})"
    depends_on: ${DEPENDS_ON}
    timeout_in_minutes: 60
    plugins:
      - docker-compose#v3.7.0:
          config:
            - .buildkite/docker-compose.${SERVICE}.yml
            - .buildkite/gen/docker-compose.${FRAMEWORK_SLUG}.${DIALECT}.yml
          run: test
          env:
            - FRAMEWORK=${FRAMEWORK}
            - RS_USERNAME=root
            - RS_PASSWORD=root
            - RS_DATABASE=test
            - RS_HOST=db
            - RS_PORT=3333
            - RS_NUM_SHARDS=${NUM_SHARDS}
            - RS_DIALECT=${DIALECT_VERSION}
            - BUILDKITE_COMMIT
      - ecr#v2.2.0:
          login: true
"""
soft_fail = """
    soft_fail:
      - exit_status: 1
"""


def generate_test_step(
    server, dialect, dialect_version, num_shards, depends_on, framework, fail
):
    service = "%s-%s%s" % (
        server.lower(),
        dialect_version,
        "sharded" if num_shards > 1 else "",
    )
    framework_slug = framework.replace("/", "_").replace(":", "_")
    step = (
        test_template.replace("${SERVER}", server)
        .replace("${DIALECT}", dialect)
        .replace("${DIALECT_VERSION}", dialect_version)
        .replace("${SERVICE}", service)
        .replace("${NUM_SHARDS}", str(num_shards))
        .replace("${FRAMEWORK}", framework)
        .replace("${FRAMEWORK_SLUG}", framework_slug)
        .replace("${DEPENDS_ON}", "%s-%s" % (depends_on, framework_slug))
    )
    if fail:
        step = step + soft_fail
    return step


push_template = """
  - key: push-${FRAMEWORK_SLUG}
    label: "Push container image for ${FRAMEWORK} on ${DIALECT_NAME}"
    command: ./.buildkite/push-image '${FRAMEWORK}'
    depends_on:
      - test-${DIALECT}-${FRAMEWORK_SLUG}
    plugins:
      - ecr#v2.2.0:
        login: true
"""


def generate_push_step(framework, dialect):
    framework_slug = framework.replace("/", "_").replace(":", "_")
    step = (
        push_template.replace("${FRAMEWORK}", framework)
        .replace("${FRAMEWORK_SLUG}", framework_slug)
        .replace("${DIALECT}", dialect)
        .replace("${DIALECT_NAME}", dialect_names[dialect])
    )
    return step


def read(path):
    f = open(path, "r")
    return f.read()


def write(path, string):
    print("--- writing ", path)
    with open(path, "w") as f:
        f.write(string)


try:
    os.makedirs(".buildkite/gen", exist_ok=True)
except FileExistsError:
    pass

fail = False
if os.environ.get("SOFT_FAIL") == "true":
    fail = True

agent = subprocess.Popen(
    ["buildkite-agent", "meta-data", "get", "changed-frameworks"],
    stdout=subprocess.PIPE,
)  # TODO:  changed-frameworks vs frameworks based on env
result = ["  - wait"]
config = json.load(agent.stdout)
for (dialect, frameworks) in config.items():
    for framework in frameworks:
        write(
            ".buildkite/gen/docker-compose.%s.%s.yml"
            % (framework.replace("/", "_").replace(":", "_"), dialect),
            compose_template.replace("${DIALECT}", dialect).replace(
                "${FRAMEWORK}", framework
            ),
        )
        result.append(generate_build_step(framework, dialect))
        for dialect_version in dialect_versions[dialect]:
            result.append(
                generate_test_step(
                    dialect_names[dialect],
                    dialect,
                    dialect_version,
                    1,
                    "build-%s" % (dialect,),
                    framework,
                    False,
                )
            )
            result.append(
                generate_test_step(
                    "readyset",
                    dialect,
                    dialect_version,
                    1,
                    "test-%s-%s" % (dialect, dialect_version),
                    framework,
                    fail,
                )
            )
result = "\n".join(result)

print(result)
print()
sys.stdout.flush()
agent = subprocess.run(
    ["buildkite-agent", "pipeline", "upload"], input=result.encode("ascii")
)
sys.exit(agent.returncode)
