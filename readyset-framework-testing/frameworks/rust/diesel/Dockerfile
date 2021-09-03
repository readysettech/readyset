FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/rust

RUN \
	sed -i 's/buster/bullseye/' /etc/apt/sources.list && \
	sed -i '/updates/d' /etc/apt/sources.list && \
	apt-get update && \
	apt-get install -y mariadb-client-10.5 libmariadb3 && \
	rm -Rvf /var/lib/apt
RUN cargo install diesel_cli --no-default-features --features mysql
RUN git clone https://github.com/mcronce/diesel.git /src/upstream

WORKDIR /src/upstream/examples/mysql
RUN \
	set -ex && \
	for dir in *; do \
		if [ -d "${dir}" ]; then \
			cd "${dir}" && \
			cargo build && \
			cd ..; \
		fi; \
	done

ARG DATABASE_URL

ADD run /src/run

ENTRYPOINT ["/src/run"]

