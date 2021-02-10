FROM alpine:3.13

RUN apk add --no-cache \
    bash \
    gawk \
    sed \
    grep \
    bc \
    coreutils \
    curl \
    netcat-openbsd \
    mongodb-tools \
    mysql-client \
    net-tools \
    postgresql-client

RUN mkdir /opt/scripts
COPY wait-for-server.sh /opt/scripts/wait-for-server.sh