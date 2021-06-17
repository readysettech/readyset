FROM python:3.9-alpine3.12

RUN apk add --no-cache mariadb-connector-c

ADD src/requirements.txt /src/requirements.txt
RUN \
	apk add --no-cache --virtual .build-deps gcc libc-dev mariadb-dev && \
	pip install --no-cache-dir -r /src/requirements.txt && \
	apk del .build-deps

ADD src /src
WORKDIR /src
ENTRYPOINT ["python3", "test.py"]

