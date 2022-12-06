# ReadySet Helm Chart

## AWS Prerequisites
Before installing the `readyset` chart, please ensure the following
prerequisites are met, with regards to your Amazon EKS cluster and RDS
instances:

### **EKS Cluster Prereqs:**

1. Create an EKS Cluster
https://docs.aws.amazon.com/eks/latest/userguide/create-cluster.html

* EKS cluster should contain a node group with at least r6a.xlarge in
  CPU/memory. ReadySet is a memory intensive application.

### Create an upstream MySQL or PostgreSQL database

An existing database can be used, the script `./scripts/setup_mysql.sh` can be
used to generate a resource with an empty MySQL database, or an RDS instance
can be created following the instructions below.

#### **Amazon RDS Prereqs:**

* An existing RDS instance which meets the following requirements:

  * Has a security group assigned that allows DB port traffic from the EKS
    worker nodes; typically 3306 for MySQL or 5432 for PostgreSQL.

  * **For MySQL RDS:**

    - RDS automated backups must be enabled. See [AWS docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html#USER_WorkingWithAutomatedBackups.Enabling).

    - RDS DB parameter group has `binlog_format` value of `row`. Please note
      that an RDS instance reboot is required after modifying this setting.

  * **For PostgreSQL RDS:**

    - RDS instance has `rds.logical_replication` value of `1`.  Please note
      that a reboot is required after modifying this setting.

To ensure a smooth deployment, please ensure that before deploying the chart,
the RDS instance is not in a `pending-reboot` state due to parameter group
changes.

2. Create a Kubernetes secret for the upstream database you'd like ReadySet to cache:

```
kubectl create secret \
  generic \
  readyset-db-url \
  --from-literal=url="${CONN_STRING}" \
  --from-literal=username="${DB_USERNAME}" \
  --from-literal=password="${DB_PASSWORD}" \
  --from-literal=database="${DB_NAME}"

```

See `scripts/create_secret.sh` for example environmental variables for
different upstream databases.

## Values Files and Deployment

Now that all the prerequisites have been taken care of, create a copy of the
`example.values.yaml` file so you may customize it. This custom values file
will be referred to as `custom.values.yaml` here on out.

A minimum set of initial values for MySQL and Postgres can also be seen below:

```
readyset:
  common:
    config:
      # -- Name of the ReadySet deployment
      deploymentName: "myapp-1"
      # -- Flag to instruct entrypoint script which adapter binary to use
      # Supported values: mysql,postgresql
      database_type: "mysql"
```

To install the chart, you can use the below command:

```
helm install readyset . --values ./custom.values.yaml
```

[comment]: # (TODO:  Update reference to Helm chart repository vs local path (`.`))

Follow the steps in the Helm installation command output to follow the status
of the release, and if all went well, to connect to the load balancer fronting
the ReadySet Adapter service.

The `mysql` and `psql` commands assume your machine has network access (VPN or
the like) to the cloud environment the chart is deployed within.

Should you need to make adjustments to your release, you can upgrade it via:

`helm upgrade readyset . --values ./custom.values.yaml`

[comment]: # (@TODO:  Update reference to Helm chart repository vs local path (`.`))

Happy caching!
