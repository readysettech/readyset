FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/python:3.9-alpine3.12

RUN apk add --no-cache mariadb-connector-c

RUN \
	apk add --no-cache --virtual .build-deps gcc libc-dev mariadb-dev && \
	pip3 install --no-cache-dir mysqlclient asyncpg && \
	apk del --no-cache .build-deps

RUN \
	apk add --no-cache --virtual .build-deps git gcc g++ musl-dev && \
	git clone https://github.com/sqlalchemy/sqlalchemy.git /src/upstream && \
	pip3 install --no-cache-dir /src/upstream && \
	apk del --no-cache .build-deps

ADD src /src
WORKDIR /src
ENTRYPOINT ["/src/run-examples-with-database-url"]

