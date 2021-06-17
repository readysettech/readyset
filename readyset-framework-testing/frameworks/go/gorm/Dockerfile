FROM golang

ADD src /src
WORKDIR /src
RUN go build .
ENTRYPOINT ["go", "run", "."]
