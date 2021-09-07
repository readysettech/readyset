FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/haskell:8.6 AS builder

RUN \
	sed -i 's/stretch/bullseye/' /etc/apt/sources.list && \
	sed -i '/updates/d' /etc/apt/sources.list && \
	apt-get update && \
	apt-get install -y libmariadb-dev-compat libpcre3-dev libghc-hsopenssl-dev ghc-8.6.5-prof

RUN cabal update

WORKDIR /src
ADD src/test.cabal src/cabal.project.local /src/
RUN cabal v2-build --dependencies-only --enable-profiling

ADD src/* /src/

RUN cabal v2-build --enable-profiling

# TODO: Update this to bullseye, leaving this pulling from Docker Hub for now.
FROM debian:stretch-slim
RUN apt-get update && apt-get install -y libgmp10 && rm -Rvf /var/lib/apt
RUN apt-get update && apt-get install -y libpcre3 && rm -Rvf /var/lib/apt
RUN \
	sed -i 's/stretch/bullseye/' /etc/apt/sources.list && \
	sed -i '/updates/d' /etc/apt/sources.list && \
	apt-get update && \
	apt-get install -y mariadb-client-10.5 libmariadb3 && \
	rm -Rvf /var/lib/apt
COPY --from=builder /src/dist-newstyle/build/x86_64-linux/ghc-8.6.5/test-0.1.0.0/x/test/build/test/test /test

ENTRYPOINT ["/test", "+RTS", "-xc"]

