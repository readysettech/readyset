# This Dockerfile is used for linting, testing, and building all of the various
# ops related code we create in this repository.
# We use Hashicorp Packer for image building. Packer includes its own linter
# and validator.
# We use Hashicorp Terraform for orchestration. Terraform includes a formatter
# and validator. tflint is there for additional checks.
# Since this needs a number of bash scripts, we also lint those using
# shellcheck.

FROM debian:bullseye-slim as fetcher

RUN set -eux; \
    apt-get update; \
    apt-get install -y curl zip xz-utils

FROM fetcher as packer-fetcher

ENV PACKER_VERSION=1.7.4
ENV PACKER_PLUGIN_AMAZON_VERSION=1.0.0

RUN set -eux; \
    cd /tmp; \
    curl -O https://releases.hashicorp.com/packer/${PACKER_VERSION}/packer_${PACKER_VERSION}_linux_amd64.zip; \
    curl -O https://releases.hashicorp.com/packer/${PACKER_VERSION}/packer_${PACKER_VERSION}_SHA256SUMS; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/packer_${PACKER_VERSION}_SHA256SUMS; \
    sha256sum -c packer_${PACKER_VERSION}_SHA256SUMS; \
    unzip /tmp/packer_${PACKER_VERSION}_linux_amd64.zip -d /usr/local/bin

# Doing this hack since Packer is not installing plugins from Hashicorp in Buildkite for some reason.
ENV PACKER_PLUGIN_AMAZON_DIR=/root/.packer.d/plugins/github.com/hashicorp/amazon
RUN set -eux; \
    cd /tmp; \
    curl -LO https://github.com/hashicorp/packer-plugin-amazon/releases/download/v${PACKER_PLUGIN_AMAZON_VERSION}/packer-plugin-amazon_v${PACKER_PLUGIN_AMAZON_VERSION}_x5.0_linux_amd64.zip; \
    curl -LO https://github.com/hashicorp/packer-plugin-amazon/releases/download/v${PACKER_PLUGIN_AMAZON_VERSION}/packer-plugin-amazon_v${PACKER_PLUGIN_AMAZON_VERSION}_SHA256SUMS; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/packer-plugin-amazon_v${PACKER_PLUGIN_AMAZON_VERSION}_SHA256SUMS; \
    sha256sum -c packer-plugin-amazon_v${PACKER_PLUGIN_AMAZON_VERSION}_SHA256SUMS; \
    mkdir -p ${PACKER_PLUGIN_AMAZON_DIR}; \
    unzip /tmp/packer-plugin-amazon_v${PACKER_PLUGIN_AMAZON_VERSION}_x5.0_linux_amd64.zip -d ${PACKER_PLUGIN_AMAZON_DIR}; \
    echo -n $(sha256sum ${PACKER_PLUGIN_AMAZON_DIR}/packer-plugin-amazon_v1.0.0_x5.0_linux_amd64 | cut -f 1 -d ' ') > \
        ${PACKER_PLUGIN_AMAZON_DIR}/packer-plugin-amazon_v1.0.0_x5.0_linux_amd64_SHA256SUM
FROM fetcher as terraform-fetcher

# These versions are bound together so putting them in the same fetcher
ENV TERRAFORM_VERSION=1.0.2
ENV TFLINT_VERSION=0.31.0
ENV TFLINT_RULESET_AWS_VERSION=0.6.0

RUN set -eux; \
    cd /tmp; \
    curl -O https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_linux_amd64.zip; \
    curl -O https://releases.hashicorp.com/terraform/${TERRAFORM_VERSION}/terraform_${TERRAFORM_VERSION}_SHA256SUMS; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/terraform_${TERRAFORM_VERSION}_SHA256SUMS; \
    sha256sum -c terraform_${TERRAFORM_VERSION}_SHA256SUMS; \
    unzip /tmp/terraform_${TERRAFORM_VERSION}_linux_amd64.zip -d /usr/local/bin

RUN set -eux; \
    cd /tmp; \
    curl -L -O https://github.com/terraform-linters/tflint/releases/download/v"${TFLINT_VERSION}"/tflint_linux_amd64.zip; \
    curl -L -O https://github.com/terraform-linters/tflint/releases/download/v"${TFLINT_VERSION}"/checksums.txt; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/checksums.txt; \
    sha256sum -c checksums.txt; \
    unzip /tmp/tflint_linux_amd64.zip -d /usr/local/bin

RUN mkdir -p ~/.tflint.d/plugins

RUN set -eux; \
    cd /tmp; \
    curl -L -O https://github.com/terraform-linters/tflint-ruleset-aws/releases/download/v"${TFLINT_RULESET_AWS_VERSION}"/tflint-ruleset-aws_linux_amd64.zip; \
    curl -L -O https://github.com/terraform-linters/tflint-ruleset-aws/releases/download/v"${TFLINT_RULESET_AWS_VERSION}"/checksums.txt; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/checksums.txt; \
    sha256sum -c checksums.txt; \
    unzip /tmp/tflint_linux_amd64.zip -d ~/.tflint.d/plugins

FROM fetcher as shellcheck-fetcher

ENV SHELLCHECK_VERSION=0.7.2

RUN set -eux; \
    cd /tmp; \
    curl -L -O https://github.com/koalaman/shellcheck/releases/download/v"${SHELLCHECK_VERSION}"/shellcheck-v"${SHELLCHECK_VERSION}".linux.x86_64.tar.xz; \
    tar xvf shellcheck-v"${SHELLCHECK_VERSION}".linux.x86_64.tar.xz; \
    cp shellcheck-v"${SHELLCHECK_VERSION}"/shellcheck /usr/local/bin

FROM debian:bullseye-slim

WORKDIR /workdir
COPY --from=packer-fetcher /usr/local/bin/packer /usr/local/bin/packer
COPY --from=packer-fetcher /root/.packer.d /root/.packer.d
COPY --from=terraform-fetcher /usr/local/bin/terraform /usr/local/bin/terraform
COPY --from=terraform-fetcher /usr/local/bin/tflint /usr/local/bin/tflint
COPY --from=terraform-fetcher /root/.tflint.d/plugins /root/.tflint.d/plugins
COPY --from=shellcheck-fetcher /usr/local/bin/shellcheck /usr/local/bin/shellcheck

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates docker.io; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN set -eux; \
    packer --version; \
    terraform --version; \
    tflint --version; \
    shellcheck --version; \
    which docker

CMD ["/bin/bash"]
