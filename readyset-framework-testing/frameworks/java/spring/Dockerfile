FROM openjdk:alpine

RUN apk add --no-update gettext jq curl
ADD src /src
WORKDIR /src
RUN ./gradlew --no-daemon assemble
RUN ./gradlew --no-daemon testClasses --stacktrace --debug

ENTRYPOINT ["/src/test"]

