REGISTRY:=305232526136.dkr.ecr.us-east-2.amazonaws.com

BASIC-IMAGE-NAME:=$(REGISTRY)/readyset-build

SERVER-IMAGE-NAME:=$(REGISTRY)/readyset-server

ADAPTER-MYSQL-IMAGE-NAME:=$(REGISTRY)/readyset-mysql

ADAPTER-PSQL-IMAGE-NAME:=$(REGISTRY)/readyset-psql

METRICS-AGGREGATOR-IMAGE-NAME:=$(REGISTRY)/metrics-aggregator

INSTALLER-IMAGE-NAME:=$(REGISTRY)/readyset-installer

build-basic: 
	docker build -t $(BASIC-IMAGE-NAME):latest -f build/Dockerfile .

build-server: 
	docker build -t $(SERVER-IMAGE-NAME):latest -f build/Dockerfile.readyset-server .

build-mysql-adapter:
	docker build -t $(ADAPTER-MYSQL-IMAGE-NAME):latest -f build/Dockerfile.readyset-mysql .

build-psql-adapter:
	docker build -t $(ADAPTER-PSQL-IMAGE-NAME):latest -f build/Dockerfile.readyset-psql .

build-metrics-aggregator: 
	docker build -t $(METRICS-AGGREGATOR-IMAGE-NAME):latest -f build/Dockerfile.metrics-aggregator .

build-installer:
	docker build -t $(INSTALLER-IMAGE-NAME):latest -f build/Dockerfile.readyset-installer .

push-basic: 
	docker push $(BASIC-IMAGE-NAME):latest

push-server: 
	docker push $(SERVER-IMAGE-NAME):latest

push-mysql-adapter:
	docker push $(ADAPTER-MYSQL-IMAGE-NAME):latest

push-psql-adapter:
	docker push $(ADAPTER-PSQL-IMAGE-NAME):latest

push-metrics-aggregator:
	docker push $(METRICS-AGGREGATOR-IMAGE-NAME):latest

push-installer:
	docker push $(INSTALLER-IMAGE-NAME):latest

nightly-tests:
	cargo run --bin noria-logictest -- verify logictests/generated
	
docker-nightly-tests:
	docker-compose -f docker-compose.yml -f build/docker-compose.ci-test.yaml run app cargo run --bin noria-logictest -- verify logictests/generated

docker-tests:
	docker-compose -f docker-compose.yml -f build/docker-compose.ci-test.yaml run app cargo test --all --exclude clustertest -- --skip integration_serial && cargo test -p noria-server integration_serial -- --test-threads=1
