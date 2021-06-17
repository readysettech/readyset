FROM node:alpine

ADD src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/src/test"]

