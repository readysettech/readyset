# ReadySet Helm Chart

## AWS Prerequisites

Before installing the `readyset` chart, please ensure the following prerequisites are met, with regards to your Amazon EKS cluster and RDS instances:

### **EKS Cluster Prereqs:**

* EKS cluster should contain a node group with at least r5.xlarge in CPU/memory. ReadySet is a memory intensive application.
* Pod placement recommendations:
  * Use a tainted node group and configure the chart to tolerate the appropriate taints.
  * We recommend using PodAntiAffinity to avoid scheduling Consul server on the same infrastructure as ReadySet's server component.

### **Amazon RDS Prereqs:**

* An existing RDS instance which meets the following requirements:

  * Has a security group assigned that allows DB port traffic from the EKS worker nodes; typically 3306 for MySQL or 5432 for PostgreSQL.

  * **For MySQL RDS:**

    - RDS automated backups must be enabled. See [AWS docs](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithAutomatedBackups.html#USER_WorkingWithAutomatedBackups.Enabling).

    - RDS DB parameter group has `binlog_format` value of `row`. Please note that an RDS instance reboot is required after modifying this setting.

  * **For PostgreSQL RDS:**

    - RDS instance has `rds.logical_replication` value of `1`.  Please note that a reboot is required after modifying this setting.

To ensure a smooth deployment, please ensure that before deploying the chart, the RDS instance is not in a `pending-reboot` state due to parameter group changes.

## AWS IAM Setup

1. Create an AWS IAM user for Vector log & metric shipping in the same region as the EKS cluster, with the following IAM policy:

    ```
    {
      "Version": "2012-10-17",
      "Statement": [
          {
              "Sid": "VisualEditor0",
              "Effect": "Allow",
              "Action": "cloudwatch:PutMetricData",
              "Resource": "*"
          },
          {
              "Sid": "VisualEditor1",
              "Effect": "Allow",
              "Action": [
                  "logs:CreateLogStream",
                  "logs:DescribeLogGroups",
                  "logs:DescribeLogStreams",
                  "logs:CreateLogGroup"
              ],
              "Resource": "arn:aws:logs:*:<AWS_ACCOUNT_ID>:log-group:*"
          },
          {
              "Sid": "VisualEditor2",
              "Effect": "Allow",
              "Action": "logs:PutLogEvents",
              "Resource": "arn:aws:logs:*:<AWS_ACCOUNT_ID>:log-group:*:log-stream:*"
          }
      ]
    }
    ```

2. Generate an IAM access key and secret key under the previously created IAM user.

    **AWS OIDC/IRSA Users:**

    If required, AWS IRSA can be leveraged to provide AWS permissions to the Vector aggregator pods.

      This can be accomplished by creating the IAM role with OIDC configured and then adding the following to your values file:

      ```
        readyset:
          common:
            secrets:
              vectorIamCredentials:
                # Blank secretName to prevent secret mounting access key/secret
                secretName: ""
          vector:
            aggregator:
              serviceAccount:
                # -- Toggles creation of Vector aggregator service account.
                create: true
                # -- Additional annotations to apply to Vector aggregator service account.
                annotations:
                  eks.amazonaws.com/role-arn: arn:aws:iam::111122223333:role/iam-role-name
      ```

## Kubernetes Secret Setup

1. Create the following Kubernetes secret using the credentials generated in the previous step.

    ```
    # readyset.common.secrets.vectorIamCredentials.secretName
    kubectl create secret \
      generic \
      readyset-vector-creds \
      --from-literal=aws_access_key_id="AKIA*****************" \
      --from-literal=aws_secret_access_key="<Secret_Access_Key>"
    ```

2. Create the following Kubernetes secret for the upstream database you'd like ReadySet to cache:

    **For Postgres upstream databases:**

    ```
    CONN_STRING=postgres://<username>:<password>@current-rds-database.csxlrghyjsz.<region>.rds.amazonaws.com:5432/<databaseName>?sslmode=disable
    DB_USERNAME="<username>"
    DB_PASSWORD="<password>"
    DB_NAME="<databaseName>"
    kubectl create secret \
      generic \
      readyset-db-url \
      --from-literal=url="${CONN_STRING}" \
      --from-literal=username="${DB_USERNAME}" \
      --from-literal=password="${DB_PASSWORD}" \
      --from-literal=database="${DB_NAME}"
    ```

    **For MySQL upstream databases:**

    ```
    CONN_STRING=mysql://<username>:<password>@current-rds-database.csxlrghyjsz.<region>.rds.amazonaws.com:3306/<databaseName>
    DB_USERNAME="<username>"
    DB_PASSWORD="<password>"
    DB_NAME="<databaseName>"
    kubectl create secret \
      generic \
      readyset-db-url \
      --from-literal=url="${CONN_STRING}" \
      --from-literal=username="${DB_USERNAME}" \
      --from-literal=password="${DB_PASSWORD}" \
      --from-literal=database="${DB_NAME}"
    ```

## Values Files and Deployment

Now that all the prerequisites have been taken care of, create a copy of the `example.values.yaml` file so you may customize it. This custom values file will be referred to as `custom.values.yaml` here on out.

A minimum set of initial values for MySQL and Postgres can also be seen below:

```
readyset:
  common:
    config:
      # -- Name of the ReadySet deployment
      deploymentName: "myapp-1"
      # -- Required to route view requests to specific regions
      region: "us-east-2"
      # -- The region where the ReadySet controller is hosted
      primaryRegion: "us-east-2"
      # -- Flag to instruct entrypoint script which adapter binary to use
      # Supported values: mysql,psql
      engine: "mysql"
      # -- Name of the CloudWatch log group that will contain ReadySet application logs
      cwLogGroupName: "readyset-logs-1"

monitoring:
  prometheusOperator:
    # -- Toggles deployment of Prometheus Operator.
    # IMPORTANT: Disable if the Prometheus operator is already running in the k8s cluster.
    enabled: true

  grafana:
    # -- Toggles deployment of Grafana Subchart
    enabled: true
    adminPassword: "readyset-monitoring"
```

To install the chart, you can use the below command:

```
helm install readyset . --values ./custom.values.yaml
```

[comment]: # (TODO:  Update reference to Helm chart repository vs local path (`.`))

Follow the steps in the Helm installation command output to follow the status of the release, and if all went well, to connect to the load balancer fronting the ReadySet Adapter service.

The `mysql` and `psql` commands assume your machine has network access (VPN or the like) to the cloud environment the chart is deployed within.

Should you need to make adjustments to your release, you can upgrade it via:

`helm upgrade readyset . --values ./custom.values.yaml`

[comment]: # (@TODO:  Update reference to Helm chart repository vs local path (`.`))

Happy caching!
