#!/usr/bin/env python3

import os
import subprocess
import sys

compose_template = """
version: '3.8'
services:
  test:
    image: "305232526136.dkr.ecr.us-east-2.amazonaws.com/frameworks/${FRAMEWORK}"
    depends_on:
      db:
        condition: service_healthy
    links:
      - db
"""

build_template = """
  - key: build-${FRAMEWORK_SLUG}
    label: "Build test container image for ${FRAMEWORK}"
    command: ./.buildkite/build-framework-image.sh '${FRAMEWORK}'
    plugins:
      - ecr#v2.2.0:
          login: true
"""


def generate_build_step(framework):
    framework_slug = framework.replace("/", "_").replace(":", "_")
    step = build_template.replace("${FRAMEWORK}", framework).replace(
        "${FRAMEWORK_SLUG}", framework_slug
    )
    return step


test_template = """
  - key: test-${SERVICE}-${FRAMEWORK_SLUG}
    label: "Test ${FRAMEWORK} against ${SERVER} ${DIALECT}"
    depends_on: ${DEPENDS_ON}
    timeout_in_minutes: 60
    plugins:
      - docker-compose#v3.7.0:
          config:
            - .buildkite/docker-compose.${SERVICE}.yml
            - .buildkite/gen/docker-compose.${FRAMEWORK_SLUG}.yml
          run: test
          env:
            - FRAMEWORK=${FRAMEWORK}
            - RS_USERNAME=root
            - RS_PASSWORD=root
            - RS_DATABASE=test
            - RS_HOST=db
            - RS_PORT=3333
            - RS_NUM_SHARDS=${NUM_SHARDS}
            - RS_DIALECT=mysql${DIALECT_SHORT}
            - BUILDKITE_COMMIT
      - ecr#v2.2.0:
          login: true
"""
soft_fail = """
    soft_fail:
      - exit_status: 1
"""


def generate_test_step(server, dialect, num_shards, depends_on, framework, fail):
    service = (
        server.lower()
        + dialect.replace(".", "")
        + ("sharded" if num_shards > 1 else "")
    )
    framework_slug = framework.replace("/", "_").replace(":", "_")
    step = (
        test_template.replace("${SERVER}", server)
        .replace("${DIALECT}", dialect)
        .replace("${DIALECT_SHORT}", dialect.replace(".", ""))
        .replace("${SERVICE}", service)
        .replace("${NUM_SHARDS}", str(num_shards))
    )
    step = (
        step.replace("${FRAMEWORK}", framework)
        .replace("${FRAMEWORK_SLUG}", framework_slug)
        .replace("${DEPENDS_ON}", depends_on + "-" + framework_slug)
    )
    if fail:
        step = step + soft_fail
    return step


push_template = """
  - key: push-${FRAMEWORK_SLUG}
    label: "Push container image for ${FRAMEWORK}"
    command: ./.buildkite/push-image '${FRAMEWORK}'
    depends_on:
      - test-mysql80-${FRAMEWORK_SLUG}
    plugins:
      - ecr#v2.2.0:
        login: true
"""


def generate_push_step(framework):
    # TODO: Actually push. Currently this is dead code.
    framework_slug = framework.replace("/", "_").replace(":", "_")
    step = push_template.replace("${FRAMEWORK}", framework).replace(
        "${FRAMEWORK_SLUG}", framework_slug
    )
    return step


def read(path):
    f = open(path, "r")
    return f.read()


def write(path, string):
    print("--- writing ", path)
    with open(path, 'w') as f:
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
for line in agent.stdout:
    line = line.decode("ascii").strip()
    write(
        ".buildkite/gen/docker-compose."
        + line.replace("/", "_").replace(":", "_")
        + ".yml",
        compose_template.replace("${FRAMEWORK}", line),
    )
    result.append(generate_build_step(line))
    result.append(generate_test_step("MySQL", "8.0", 1, "build", line, False))
    if os.environ.get("NIGHTLY") == "true":
        result.append(
            generate_test_step("readyset", "8.0", 1, "test-mysql80", line, fail)
        )
result = "\n".join(result)

print(result)
print()
sys.stdout.flush()
agent = subprocess.run(
    ["buildkite-agent", "pipeline", "upload"], input=result.encode("ascii")
)
sys.exit(agent.returncode)
