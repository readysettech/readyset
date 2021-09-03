FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/golang

ADD src /src
WORKDIR /src
RUN go build .
ENTRYPOINT ["go", "run", "."]
