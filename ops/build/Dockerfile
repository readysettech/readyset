# This Dockerfile is used for linting, testing, and building all of the various
# images we create in multiple forms.
# We use Hashicorp Packer for image building. Packer includes its own linter
# and validator.
# We use Ansible as the provisioner.
# To lint, the project ansible-lint is brought in along with yamllint.
# For testing, molecule is the system for doing so.
# This Dockerfile is inspired by the Dockerfile for the Ansible Toolset
# https://github.com/ansible-community/toolset/blob/main/Dockerfile

FROM debian:bullseye-slim as ansible-builder

WORKDIR /workdir

RUN set -eux; \
    apt-get update; \
    apt-get install -y rsync ssh libyaml-dev zip curl python3 python3-venv

COPY requirements.txt /tmp/requirements.txt
RUN set -eux; \
    python3 -m venv /opt/ansible; \
    /opt/ansible/bin/python3 -m pip install --upgrade pip wheel; \
    /opt/ansible/bin/python3 -m pip install -r /tmp/requirements.txt

FROM debian:bullseye-slim as packer-builder

ENV PACKER_VERSION=1.7.4

RUN set -eux; \
    apt-get update; \
    apt-get install -y curl zip; \
    cd /tmp; \
    curl -O https://releases.hashicorp.com/packer/${PACKER_VERSION}/packer_${PACKER_VERSION}_linux_amd64.zip; \
    curl -O https://releases.hashicorp.com/packer/${PACKER_VERSION}/packer_${PACKER_VERSION}_SHA256SUMS; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/packer_${PACKER_VERSION}_SHA256SUMS; \
    sha256sum -c packer_${PACKER_VERSION}_SHA256SUMS; \
    unzip /tmp/packer_${PACKER_VERSION}_linux_amd64.zip -d /usr/local/bin

FROM debian:bullseye-slim as terraform-builder

# These versions are bound together so putting them in the same builder
ENV TERRAFORM_VERSION=1.0.2
ENV TFLINT_VERSION=0.31.0
ENV TFLINT_RULESET_AWS_VERSION=0.6.0

RUN set -eux; \
    apt-get update; \
    apt-get install -y curl zip; \
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
    cat checksums.txt; \
    sed -i '/.*linux_amd64.zip/!d' /tmp/checksums.txt; \
    cat checksums.txt; \
    sha256sum -c checksums.txt; \
    unzip /tmp/tflint_linux_amd64.zip -d ~/.tflint.d/plugins

FROM debian:bullseye-slim

WORKDIR /workdir
COPY --from=ansible-builder /opt/ansible /opt/ansible
COPY --from=packer-builder /usr/local/bin/packer /usr/local/bin/packer
COPY --from=terraform-builder /usr/local/bin/terraform /usr/local/bin/terraform
COPY --from=terraform-builder /usr/local/bin/tflint /usr/local/bin/tflint
COPY --from=terraform-builder /root/.tflint.d/plugins /root/.tflint.d/plugins

ENV PATH=$PATH:/opt/ansible/bin

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-venv docker.io; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN ls -la /opt/ansible/bin

RUN set -eux; \
    packer --version; \
    terraform --version; \
    tflint --version; \
    /opt/ansible/bin/python3 -m pip check; \
    ansible --version; \
    yamllint --version; \
    ansible-lint --version; \
    molecule --version; \
    molecule drivers; \
    which docker

CMD ["/bin/bash"]
