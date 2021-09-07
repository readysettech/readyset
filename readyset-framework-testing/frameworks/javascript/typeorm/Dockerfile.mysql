FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/node:alpine

RUN apk add --no-cache gettext

ADD src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/src/test"]

