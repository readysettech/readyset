# Overview
This docker stack spins up ReadySet's monitoring services locally. It can be used to test Grafana and Prometheus integrations. 

This docker stack is provided in order to facilitate testing the experiments with Grafana integration.

## Components
The following components are present in this stack

| Service | Port | User | Password | Tools configured |
|------|---------|------|---------|---------|
| Grafana | 4000 | -- | -- | <ul><li>'Experiments' dashboard</li></ul> |
| Prometheus | 9090 | | | |

## Configurations
### Grafana
If you want to extend the current Grafana configurations, check:
- `grafana/dashboards`: This folder is where all the dashboard definitions are stored, as separate json files.
- `grafana/provisioning/dashboards/default.yaml`: This file configures which dashboards should be pre-loaded by Grafana.
- `grafana/provisioning/datasources/default.yaml`: This file configures which datasources should be pre-loaded by Grafana.

### Prometheus
Prometheus's config lives in `prometheus/prometheus.yml`. 
By default, prometheus only pulls metrics from "localhost:6033/prometheus", the default prometheus endpoint
for noria-server.
