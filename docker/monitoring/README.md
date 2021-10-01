# Overview
This docker stack spins up ReadySet's monitoring services locally. It can be used to test InfluxDb, Grafana,
and Prometheus integrations. 

This docker stack is provided in order to facilitate testing the experiments with InfluxDB/Grafana integration.

## Components
The following components are present in this stack

| Service | Port | User | Password | Tools configured |
|------|---------|------|---------|---------|
| Grafana | 3000 | admin | admin | <ul><li>'Experiments' dashboard</li><li>InfluxDB datasource</li></ul> |
| InfluxDB | 8086 | username | password | <ul><li>'test' database</li></ul>|
| Prometheus | 9090 | | | |

## Configurations
### Grafana
If you want to extend the current Grafana configurations, check:
- `grafana/dashboards`: This folder is where all the dashboard definitions are stored, as separate json files.
- `grafana/provisioning/dashboards/default.yaml`: This file configures which dashboards should be pre-loaded by Grafana.
- `grafana/provisioning/datasources/default.yaml`: This file configures which datasources should be pre-loaded by Grafana.

### InfluxDB
If you want to add more initialization scripts to InfluxDB (i.e, to create more databases), simply add a `*.sh` or `*.iql` file into the `influx/docker-entrypoint-initdb.d` folder

### Prometheus
Prometheus's config lives in `prometheus/prometheus.yml`. 
By default, prometheus only pulls metrisc from "localhost:6033/prometheus", the default prometheus endpoint
for noria-server.
