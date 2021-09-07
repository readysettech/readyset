FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/node:alpine

ADD src /src
WORKDIR /src
RUN npm install
ENTRYPOINT ["/src/test"]

