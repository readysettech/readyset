## Requirements

No requirements.

## Providers

| Name | Version |
|------|---------|
| aws | n/a |
| null | n/a |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| aws\_region | The AWS region to create resources in. | `string` | n/a | yes |
| chart\_version\_defaults | The default versions for Helm charts that will be deployed along side the Kubernetes cluster. | `map(any)` | <pre>{<br>  "cluster_autoscaler": "9.12.0",<br>  "external_dns": "2.20.3",<br>  "node_term_handler": "0.16.0"<br>}</pre> | no |
| chart\_versions | Chart version override map. Setting values here will override what's set in chart\_version\_defaults. | `map(any)` | `{}` | no |
| cluster\_autoscaler\_enabled | Toggles provisioning of the Cluster Autoscaler's related resources. | `bool` | `false` | no |
| cluster\_endpoint\_private\_only\_access | Toggles private-only access mechanisms for the EKS cluster. | `bool` | `true` | no |
| cluster\_ip\_family | The IP family used to assign Kubernetes pod and service addresses. Valid values are `ipv4` (default) and `ipv6`. You can only specify an IP family when you create a cluster, changing this value will force a new cluster to be created! | `string` | `"ipv4"` | no |
| cluster\_log\_retention\_days | Number of days for k8s CloudWatch logs to be retained. | `number` | `14` | no |
| cluster\_log\_types | A list of log object types for k8s to log to CloudWatch. | `list(string)` | <pre>[<br>  "audit",<br>  "api",<br>  "authenticator"<br>]</pre> | no |
| cluster\_name | Name of EKS cluster to be created. | `string` | n/a | yes |
| cluster\_service\_ipv4\_cidr | IPv4 CIDR block where k8s service will be provisioned from. This is a virtual network. | `string` | `"10.100.0.0/16"` | no |
| cluster\_version | Version of the EKS cluster. | `string` | `"1.21.2"` | no |
| create\_subnet\_elb\_tags | Toggles creation of necessary subnet tags which underpin auto-subnet discovery of LB Ingress Controller. | `bool` | `true` | no |
| external\_dns\_external\_enabled | Toggles provisioning of the External-DNS (for public hosted zones) related resources. | `bool` | `false` | no |
| external\_dns\_internal\_enabled | Toggles provisioning of the External-DNS (for private hosted zones) related resources. | `bool` | `false` | no |
| external\_dns\_private\_zone\_domain | Private hosted zone domain to use for ExternalDNS private DNS records. | `string` | `""` | no |
| external\_dns\_pub\_zone\_domain | Public hosted zone domain to use for ExternalDNS public DNS records. | `string` | `""` | no |
| kms\_key\_arn | ARN of KMS to be used for wrapper encryption of k8s secrets. | `string` | n/a | yes |
| node\_termination\_handler\_enabled | Toggles provisioning of the Node Termination Handler's related resources. | `bool` | `false` | no |
| resource\_tags | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| self\_managed\_node\_groups | Worker node configurations for the cluster. | `map(any)` | <pre>{<br>  "main": {<br>    "desired_size": 1,<br>    "instance_refresh": {<br>      "preferences": {<br>        "checkpoint_delay": 600,<br>        "checkpoint_percentages": [<br>          35,<br>          70,<br>          100<br>        ],<br>        "instance_warmup": 300,<br>        "min_healthy_percentage": 50<br>      },<br>      "strategy": "Rolling",<br>      "triggers": [<br>        "tag"<br>      ]<br>    },<br>    "instance_type": "m5.large",<br>    "max_size": 2,<br>    "propogate_tags": [<br>      {<br>        "key": "aws-node-termination-handler/managed",<br>        "propagate_at_launch": true,<br>        "value": true<br>      }<br>    ]<br>  }<br>}</pre> | no |
| vpc\_id | ID of the VPC to deploy resources within. | `string` | n/a | yes |
| worker\_subnet\_ids | Subnet ids for placement of EKS worker nodes. | `list(string)` | n/a | yes |
| workers\_ssh\_cidr\_allowed | CIDR ranges to allow SSH from. These should be non-publicly routable CIDR blocks. | `list` | `[]` | no |

## Outputs

| Name | Description |
|------|-------------|
| cluster\_arn | ARN of the created EKS cluster. |
| cluster\_endpoint | Endpoint for the Kubernetes API server. |

