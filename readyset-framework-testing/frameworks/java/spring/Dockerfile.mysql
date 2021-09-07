FROM 305232526136.dkr.ecr.us-east-2.amazonaws.com/mirror/openjdk:alpine

RUN apk add --no-update gettext jq curl
ADD src /src
WORKDIR /src
RUN ./gradlew --no-daemon assemble
RUN ./gradlew --no-daemon testClasses --stacktrace --debug

ENTRYPOINT ["/src/test"]

