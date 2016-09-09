#!/bin/sh
echo "Build"
cargo build --bin web
res=$?
if [ "$res" -ne 0 ]; then
	exit "$res"
fi

echo "Start"
cargo run --bin web >/dev/null &
pid=$(pgrep target/debug/web)

# wait for server to be ready
sleep 1

echo "Seed"
curl --data-raw '{"id": 1, "title": "hello"}' -H "Content-Type: application/json" localhost:8080/article
curl --data-raw '{"id": 2, "title": "world"}' -H "Content-Type: application/json" localhost:8080/article
curl --data-raw '{"user": 1, "id": 1}' -H "Content-Type: application/json" localhost:8080/vote
curl --data-raw '{"user": 1, "id": 2}' -H "Content-Type: application/json" localhost:8080/vote
curl --data-raw '{"user": 2, "id": 2}' -H "Content-Type: application/json" localhost:8080/vote
curl --data-raw '{"user": 3, "id": 1}' -H "Content-Type: application/json" localhost:8080/vote
curl --data-raw '{"user": 3, "id": 2}' -H "Content-Type: application/json" localhost:8080/vote

json() {
	if command -v jq >/dev/null 2>&1; then
		jq .
	else
		cat
	fi
}

echo "All"
curl -s localhost:8080/awvc | json
echo "Where id=1"
curl -s "localhost:8080/awvc?id=1" | json
echo "Where title=world"
curl -s "localhost:8080/awvc?title=world" | json
echo "Where id=1 && title=hello"
curl -s "localhost:8080/awvc?id=1&title=hello" | json
echo "Where id=2 && title=hello"
curl -s "localhost:8080/awvc?id=2&title=hello" | json

kill "$pid" 2>/dev/null >/dev/null
wait "$pid" 2>/dev/null >/dev/null
