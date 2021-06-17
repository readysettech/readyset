FROM node:alpine

RUN apk add --no-cache gettext

ADD src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/src/test"]

