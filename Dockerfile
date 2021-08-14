FROM rust:latest as builder
WORKDIR /workdir
COPY . .
RUN cargo build --all

FROM rust:latest as runtime
WORKDIR /workdir
COPY --from=builder /workdir/target/debug/xtask /usr/local/bin/xtask

RUN curl -o terraform.zip https://releases.hashicorp.com/terraform/1.0.2/terraform_1.0.2_linux_amd64.zip && \
    echo "7329f887cc5a5bda4bedaec59c439a4af7ea0465f83e3c1b0f4d04951e1181f4 terraform.zip" | sha256sum -c - && \ 
    unzip -d /usr/local/bin terraform.zip && \
    rm terraform.zip

RUN curl -o /usr/local/bin/sops -L https://github.com/mozilla/sops/releases/download/v3.7.1/sops-v3.7.1.linux && \
    echo "185348fd77fc160d5bdf3cd20ecbc796163504fd3df196d7cb29000773657b74 /usr/local/bin/sops" | sha256sum -c && \
    chmod a+x /usr/local/bin/sops

ENTRYPOINT [ "xtask" ]
