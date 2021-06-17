FROM alpine

RUN apk add --no-cache mysql-client mariadb-connector-c

RUN mkdir /src
WORKDIR /src
RUN \
	apk add --no-cache --virtual .build-deps curl jq && \
	curl -LO "$(\
		curl -s https://api.github.com/repos/skeema/skeema/releases/latest | \
			# Split into multiple jq commans in separate lines for readability \
			jq -r '.assets[]' | \
			jq -r 'select(.content_type == "application/gzip")' | \
			jq -r 'select (.name | contains("linux_amd64")) | .browser_download_url' \
	)" && \
	tar -zxvf *.tar.gz && \
	mv -vf skeema /usr/bin/ && \
	rm -Rvf * && \
	apk del --no-cache .build-deps

ADD run test.sql /src/
ADD test.step1 /src/test.step1
ADD test.step2 /src/test.step2
ENTRYPOINT ["/src/run"]

