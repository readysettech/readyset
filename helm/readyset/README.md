# ReadySet Helm Chart

> :exclamation: This chart is currently alpha-level quality

## Overview

This is the official ReadySet Helm chart for deploying a ReadySet cluster on Kubernetes.

## Kubernetes platform support

This chart aims to install with minimal configuration required for both GKE and EKS currently. We are soon expanding our testing suite to cover other providers and configurations and we will document a support matrix soon.

## Preconfiguration

Assuming you have a Kubernetes cluster that you are able to deploy apps to with Helm you will need to configure a kubernetes secret that contains the information needed for ReadySet to connect to your upstream database. You can configure this secret with the follow block

```
export READYSET_USER=your-username
export READYSET_PASS=your-password
export DATABASE_NAME=your-database-name
export DATABASE_HOST=your-database-host
export DATABASE_TYPE=your-database-type # mysql OR postgresql
export DATABASE_URI="${DATABASE_TYPE}://${READYSET_USER}:${READYSET_PASS}@${DATABASE_HOST}/${DATABASE_NAME}"

# The name `readyset-upstream-database` is expected by the chart, do not rename it!
kubectl create secret generic readyset-upstream-database \
    --from-literal=url="${DATABASE_TYPE}://${READYSET_USER}:${READYSET_PASS}@${DATABASE_HOST}/${DATABASE_NAME}" \
    --from-literal=host="${DATABASE_HOST}" \
    --from-literal=username="${READYSET_USER}" \
    --from-literal=password=${READYSET_PASS} \
    --from-literal=database=${DATABASE_NAME} \
```

## Add the repo

Provided you have Helm installed, a simple
```
helm repo add readyset https://helm.releases.readyset.io
```
will configure the ReadySet Helm repository

### Search the repo for releases

You can search the ReadySet repo explicitly by
```
$ helm search repo readyset/readyset
NAME                CHART VERSION   APP VERSION   DESCRIPTION
readyset/readyset   0.2.0           latest        Official ReadySet Chart
```
or you can search via ArtifactHub
```
$ helm search hub readyset
URL                                                  CHART VERSION   APP VERSION   DESCRIPTION
https://artifacthub.io/packages/helm/readyset/r...   0.2.0           latest        Official ReadySet Chart
```

## Deploying the chart

```
helm install readyset readyset/readyset --version=0.2.0 --dry-run
```
See below for configuring the cluster beyond the default values.

## Configuring the chart

To configure the chart for your environment, you can run the following command
```
helm show values readyset/readyset > values.yaml
```
to dump a yaml document of the default configuration values. Edit them to your liking and deploy the chart as described above but add the values file like the following example
```
helm install readyset readyset/readyset --version=0.2.0 --values=values.yaml
```

## When the chart has successfully deployed

A message will be displayed with instructions on connecting to your ReadySet instance. They will look similar to the following:

```
Congrats, ReadySet has been deployed!!

CHART NAME: readyset
CHART VERSION: 0.3.0
APP VERSION: latest

Give the chart approximately 5 minutes to deploy. When the service is ready, you should see all pods up.

For connecting to the readyset-adapter via MySQL:

mysql -u $(kubectl get secret readyset-upstream-database -o jsonpath="{.data.username}" | base64 -d) \
  -h $(kubectl get svc readyset-adapter --template "{{ range (index .status.loadBalancer.ingress 0) }}{{.}}{{ end }}") \
  --password=$(kubectl get secret readyset-upstream-database -o jsonpath="{.data.password}" | base64 -d) \
  --database=$(kubectl get secret readyset-upstream-database -o jsonpath="{.data.database}" | base64 -d)
```
