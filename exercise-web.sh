#!/bin/sh
echo "Build"
cargo build --bin web --features=binaries,web
res=$?
if [ "$res" -ne 0 ]; then
	exit "$res"
fi

echo "Start"
cargo run --bin web --features=binaries,web >/dev/null &
sleep 1
pid=$(pgrep -f target/debug/web)

# wait for server to be ready
sleep 1

echo "Seed"
put() {
  curl --data-raw "$2" -H "Content-Type: application/json" "localhost:8080/$1"
}

put article '{"id": 1, "title": "hello", "user": 1, "url": "http://example.com/1"}'
put article '{"id": 2, "title": "world", "user": 2, "url": "http://example.com/2"}'
put vote '{"user": 1, "id": 1}'
put vote '{"user": 1, "id": 2}'
put vote '{"user": 2, "id": 2}'
put vote '{"user": 3, "id": 1}'
put vote '{"user": 3, "id": 2}'

json() {
	if command -v jq >/dev/null 2>&1; then
		jq .
	else
		cat
	fi
}

#echo "All"
#curl -s localhost:8080/awvc | json
echo "Where id=1"
curl -s "localhost:8080/awvc?key=1" | json

kill "$pid" 2>/dev/null >/dev/null
wait "$pid" 2>/dev/null >/dev/null
