REGISTRY:=069491470376.dkr.ecr.us-east-2.amazonaws.com

BASIC-IMAGE-NAME:=$(REGISTRY)/noria-build

SERVER-IMAGE-NAME:=$(REGISTRY)/noria-server

ADAPTER-IMAGE-NAME:=$(REGISTRY)/noria-mysql

build-basic: 
	docker build -t $(BASIC-IMAGE-NAME):latest -f build/Dockerfile .

build-server: 
	docker build -t $(SERVER-IMAGE-NAME):latest -f build/Dockerfile.noria-server .

build-adapter: 
	docker build -t $(ADAPTER-IMAGE-NAME):latest -f build/Dockerfile.noria-mysql .

push-basic: 
	docker push $(BASIC-IMAGE-NAME):latest

push-server: 
	docker push $(SERVER-IMAGE-NAME):latest

push-adapter: 
	docker push $(ADAPTER-IMAGE-NAME):latest

nightly-tests:
	cargo run --bin noria-logictest -- verify logictests/generated
	
docker-nightly-tests:
	docker-compose -f docker-compose.yml -f build/docker-compose.ci-test.yaml run app cargo run --bin noria-logictest -- verify logictests/generated

docker-tests:
	docker-compose -f docker-compose.yml -f build/docker-compose.ci-test.yaml run app cargo test --all --exclude clustertest -- --skip integration_serial && cargo test -p noria-server integration_serial -- --test-threads=1
