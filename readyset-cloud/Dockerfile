FROM ubuntu:22.04

ENV TERRAFORM_VERSION="1.2.4" \
    SOPS_VERSION="3.7.3"

RUN set -eux; \
    apt-get update; \
    apt-get install -y age curl gnupg; \
    curl -fsSL https://apt.releases.hashicorp.com/gpg | gpg --dearmor > /usr/share/keyrings/hashicorp-archive-keyring.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com jammy main" > /etc/apt/sources.list.d/hashicorp.list; \
    apt-get update; \
    apt-get install terraform=${TERRAFORM_VERSION}; \
    curl -fsSL -o /tmp/sops.deb https://github.com/mozilla/sops/releases/download/v${SOPS_VERSION}/sops_${SOPS_VERSION}_amd64.deb; \
    dpkg -i /tmp/sops.deb; \
    groupadd -r readyset-cloud && useradd --no-log-init -r -m -g readyset-cloud readyset-cloud

USER readyset-cloud

ENV SHELL=/bin/bash \
    PNPM_HOME="/home/readyset-cloud/.local/share/pnpm"

ENV PATH="${PNPM_HOME}:${PATH}"

WORKDIR /home/readyset-cloud

RUN set -eux; \
    curl -fsSL https://get.pnpm.io/install.sh | sh -; \
    pnpm env use --global lts; \
    mkdir app

WORKDIR /home/readyset-cloud/app

COPY --chown=readyset-cloud:readyset-cloud pnpm-lock.yaml ./

RUN set -eux; \
    pnpm fetch --dev; \
    pnpm fetch

COPY --chown=readyset-cloud:readyset-cloud . ./

RUN pnpm install --recursive --offline
