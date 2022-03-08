# EKS-Cluster Terraform Modules

This is the main Terraform module which orchestrates provisioning an EKS cluster which, via a handful of variable toggles, can be bootstrapped with foundational Helminto a highly automated and full-featured k8s cluster

## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.72.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 3.72.0 |
| <a name="provider_kubernetes"></a> [kubernetes](#provider\_kubernetes) | n/a |
| <a name="provider_null"></a> [null](#provider\_null) | n/a |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_aws-lb-controller"></a> [aws-lb-controller](#module\_aws-lb-controller) | ./modules/aws-lb-controller | n/a |
| <a name="module_benchmark-prom-grafana"></a> [benchmark-prom-grafana](#module\_benchmark-prom-grafana) | ./modules/prom-grafana | n/a |
| <a name="module_cluster-autoscaler"></a> [cluster-autoscaler](#module\_cluster-autoscaler) | ./modules/cluster-autoscaler | n/a |
| <a name="module_eks"></a> [eks](#module\_eks) | terraform-aws-modules/eks/aws | ~> 18.5.1 |
| <a name="module_externaldns-internal"></a> [externaldns-internal](#module\_externaldns-internal) | ./modules/external-dns | n/a |
| <a name="module_ips-refresher"></a> [ips-refresher](#module\_ips-refresher) | ./modules/ips-refresher | n/a |
| <a name="module_k8s-ingress-internal"></a> [k8s-ingress-internal](#module\_k8s-ingress-internal) | ./modules/k8s-ingress | n/a |
| <a name="module_node-local-dns"></a> [node-local-dns](#module\_node-local-dns) | ./modules/node-local-dns | n/a |
| <a name="module_node-term-handler"></a> [node-term-handler](#module\_node-term-handler) | ./modules/node-termination-handler | n/a |
| <a name="module_vpc-subnet-tags"></a> [vpc-subnet-tags](#module\_vpc-subnet-tags) | ./modules/eks-vpc-tags | n/a |

## Resources

| Name | Type |
|------|------|
| [aws_security_group.supplemental](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group) | resource |
| [kubernetes_namespace.this](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/namespace) | resource |
| [null_resource.apply](https://registry.terraform.io/providers/hashicorp/null/latest/docs/resources/resource) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_eks_cluster_auth.this](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/eks_cluster_auth) | data source |
| [aws_region.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/region) | data source |
| [aws_route53_zone.private](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/route53_zone) | data source |
| [aws_route53_zone.public](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/route53_zone) | data source |
| [aws_ssm_parameter.grafana_pass](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/ssm_parameter) | data source |
| [aws_subnet_ids.private](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet_ids) | data source |
| [aws_subnet_ids.public](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet_ids) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_alb_acm_cert_arn"></a> [alb\_acm\_cert\_arn](#input\_alb\_acm\_cert\_arn) | ACM certificate arn to be applied to ingress' listeners. | `string` | `""` | no |
| <a name="input_alb_internal_ingress_enabled"></a> [alb\_internal\_ingress\_enabled](#input\_alb\_internal\_ingress\_enabled) | Toggles deployment of Internal load balancer ingress definitions. | `bool` | `true` | no |
| <a name="input_aws_lb_controller_enabled"></a> [aws\_lb\_controller\_enabled](#input\_aws\_lb\_controller\_enabled) | Toggles provisioning of the AWS-LB-Controller module's resources. | `bool` | `true` | no |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_benchmark_prom_grafana_enabled"></a> [benchmark\_prom\_grafana\_enabled](#input\_benchmark\_prom\_grafana\_enabled) | Toggles provisioning of the benchmarking initiative's Prometheus and Grafana related resources. | `bool` | `false` | no |
| <a name="input_benchmark_prom_pushgw_enabled"></a> [benchmark\_prom\_pushgw\_enabled](#input\_benchmark\_prom\_pushgw\_enabled) | Toggles creation of Prometheus Push Gateway resources for benchmarking. | `bool` | `true` | no |
| <a name="input_benchmark_prom_pushgw_replicas"></a> [benchmark\_prom\_pushgw\_replicas](#input\_benchmark\_prom\_pushgw\_replicas) | Controls number of replicas of Prometheus Push Gateway. | `number` | `3` | no |
| <a name="input_benchmark_ssm_path_grafana_password"></a> [benchmark\_ssm\_path\_grafana\_password](#input\_benchmark\_ssm\_path\_grafana\_password) | SSM path suffix to Grafana password. Will be prefixed with cluster name automatically. | `string` | `"/benchmark/grafana/adminPassword"` | no |
| <a name="input_chart_version_defaults"></a> [chart\_version\_defaults](#input\_chart\_version\_defaults) | The default versions for Helm charts that will be deployed along side the Kubernetes cluster. | `map(any)` | <pre>{<br>  "aws_lb_controller": "1.3.3",<br>  "bench_prom_grafana": "32.2.0",<br>  "cluster_autoscaler": "9.12.0",<br>  "external_dns": "6.1.4",<br>  "node_term_handler": "0.16.0"<br>}</pre> | no |
| <a name="input_chart_versions"></a> [chart\_versions](#input\_chart\_versions) | Chart version override map. Setting values here will override what's set in chart\_version\_defaults. | `map(any)` | `{}` | no |
| <a name="input_cluster_autoscaler_enabled"></a> [cluster\_autoscaler\_enabled](#input\_cluster\_autoscaler\_enabled) | Toggles provisioning of the Cluster Autoscaler's related resources. | `bool` | `false` | no |
| <a name="input_cluster_endpoint_private_only_access"></a> [cluster\_endpoint\_private\_only\_access](#input\_cluster\_endpoint\_private\_only\_access) | Toggles private-only access mechanisms for the EKS cluster. | `bool` | `true` | no |
| <a name="input_cluster_ip_family"></a> [cluster\_ip\_family](#input\_cluster\_ip\_family) | The IP family used to assign Kubernetes pod and service addresses. Valid values are `ipv4` (default) and `ipv6`. You can only specify an IP family when you create a cluster, changing this value will force a new cluster to be created! | `string` | `"ipv4"` | no |
| <a name="input_cluster_log_retention_days"></a> [cluster\_log\_retention\_days](#input\_cluster\_log\_retention\_days) | Number of days for k8s CloudWatch logs to be retained. | `number` | `14` | no |
| <a name="input_cluster_log_types"></a> [cluster\_log\_types](#input\_cluster\_log\_types) | A list of log object types for k8s to log to CloudWatch. | `list(string)` | <pre>[<br>  "audit",<br>  "api",<br>  "authenticator"<br>]</pre> | no |
| <a name="input_cluster_name"></a> [cluster\_name](#input\_cluster\_name) | Name of EKS cluster to be created. | `string` | n/a | yes |
| <a name="input_cluster_service_ipv4_cidr"></a> [cluster\_service\_ipv4\_cidr](#input\_cluster\_service\_ipv4\_cidr) | IPv4 CIDR block where k8s service will be provisioned from. This is a virtual network. | `string` | `"10.100.0.0/16"` | no |
| <a name="input_cluster_version"></a> [cluster\_version](#input\_cluster\_version) | Version of the EKS cluster. | `string` | `"1.21"` | no |
| <a name="input_cluster_vpn_cidr_blocks"></a> [cluster\_vpn\_cidr\_blocks](#input\_cluster\_vpn\_cidr\_blocks) | Ingress IPv4 CIDR block where VPN traffic originates while hitting the k8s control plane. | `list(string)` | `[]` | no |
| <a name="input_create_subnet_elb_tags"></a> [create\_subnet\_elb\_tags](#input\_create\_subnet\_elb\_tags) | Toggles creation of necessary subnet tags which underpin auto-subnet discovery of LB Ingress Controller. | `bool` | `true` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment. | `string` | n/a | yes |
| <a name="input_external_dns_external_enabled"></a> [external\_dns\_external\_enabled](#input\_external\_dns\_external\_enabled) | Toggles provisioning of the External-DNS (for public hosted zones) related resources. | `bool` | `false` | no |
| <a name="input_external_dns_internal_enabled"></a> [external\_dns\_internal\_enabled](#input\_external\_dns\_internal\_enabled) | Toggles provisioning of the External-DNS (for private hosted zones) related resources. | `bool` | `false` | no |
| <a name="input_external_dns_internal_zone_mode"></a> [external\_dns\_internal\_zone\_mode](#input\_external\_dns\_internal\_zone\_mode) | Determines what DNS zone the internal deployment will use. Can be used with a public zone. | `string` | `"private"` | no |
| <a name="input_external_dns_private_zone_domain"></a> [external\_dns\_private\_zone\_domain](#input\_external\_dns\_private\_zone\_domain) | Private hosted zone domain to use for ExternalDNS private DNS records. | `string` | `""` | no |
| <a name="input_external_dns_pub_zone_domain"></a> [external\_dns\_pub\_zone\_domain](#input\_external\_dns\_pub\_zone\_domain) | Public hosted zone domain to use for ExternalDNS public DNS records. | `string` | `""` | no |
| <a name="input_ips_refresher_authorized_ecr_resource_arns"></a> [ips\_refresher\_authorized\_ecr\_resource\_arns](#input\_ips\_refresher\_authorized\_ecr\_resource\_arns) | IAM resource list of ECR ARNs to grant read-access to. Supports wildcarding. | `list` | <pre>[<br>  "*"<br>]</pre> | no |
| <a name="input_ips_refresher_ecr_aws_account_id"></a> [ips\_refresher\_ecr\_aws\_account\_id](#input\_ips\_refresher\_ecr\_aws\_account\_id) | AWS account ID which ECR repositories exist. There is currently a 1:1 relationship between ECR account and IPS-refresher deployments. | `string` | `"305232526136"` | no |
| <a name="input_ips_refresher_enabled"></a> [ips\_refresher\_enabled](#input\_ips\_refresher\_enabled) | Toggles the deployment of the IPS-refresher Helm chart. This provides creds to k8s when pulling from private ECR repos. | `bool` | `true` | no |
| <a name="input_kms_key_arn"></a> [kms\_key\_arn](#input\_kms\_key\_arn) | ARN of KMS to be used for wrapper encryption of k8s secrets. | `string` | n/a | yes |
| <a name="input_kubernetes_namespaces"></a> [kubernetes\_namespaces](#input\_kubernetes\_namespaces) | List of Kubernetes namespaces to be created by the module inside the EKS cluster. | `list(string)` | `[]` | no |
| <a name="input_node_termination_handler_enabled"></a> [node\_termination\_handler\_enabled](#input\_node\_termination\_handler\_enabled) | Toggles provisioning of the Node Termination Handler's related resources. | `bool` | `false` | no |
| <a name="input_ns_ingress_routing_rules"></a> [ns\_ingress\_routing\_rules](#input\_ns\_ingress\_routing\_rules) | Map of maps representing per-namespace ingress hosts to be provisioned. | `map(any)` | n/a | yes |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| <a name="input_self_managed_node_group_defaults"></a> [self\_managed\_node\_group\_defaults](#input\_self\_managed\_node\_group\_defaults) | Map of self-managed node group default configurations. | `any` | n/a | yes |
| <a name="input_self_managed_node_groups"></a> [self\_managed\_node\_groups](#input\_self\_managed\_node\_groups) | Worker node configurations for the cluster. | `any` | <pre>{<br>  "main": {<br>    "desired_size": 1,<br>    "instance_refresh": {<br>      "preferences": {<br>        "checkpoint_delay": 600,<br>        "checkpoint_percentages": [<br>          35,<br>          70,<br>          100<br>        ],<br>        "instance_warmup": 240,<br>        "min_healthy_percentage": 50<br>      },<br>      "strategy": "Rolling",<br>      "triggers": [<br>        "tag"<br>      ]<br>    },<br>    "instance_type": "m5.large",<br>    "max_size": 2,<br>    "propogate_tags": [<br>      {<br>        "key": "aws-node-termination-handler/managed",<br>        "propagate_at_launch": true,<br>        "value": true<br>      }<br>    ]<br>  }<br>}</pre> | no |
| <a name="input_vpc_dns_resolver_ip"></a> [vpc\_dns\_resolver\_ip](#input\_vpc\_dns\_resolver\_ip) | Private IP of the VPC DNS resolver which is hosting the k8s cluster. | `string` | `""` | no |
| <a name="input_vpc_id"></a> [vpc\_id](#input\_vpc\_id) | ID of the VPC to deploy resources within. | `string` | n/a | yes |
| <a name="input_worker_subnet_ids"></a> [worker\_subnet\_ids](#input\_worker\_subnet\_ids) | Subnet ids for placement of EKS worker nodes. | `list(string)` | n/a | yes |
| <a name="input_workers_ssh_cidr_allowed"></a> [workers\_ssh\_cidr\_allowed](#input\_workers\_ssh\_cidr\_allowed) | CIDR ranges to allow SSH from. These should be non-publicly routable CIDR blocks. | `list` | `[]` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_cluster_arn"></a> [cluster\_arn](#output\_cluster\_arn) | ARN of the created EKS cluster. |
| <a name="output_cluster_endpoint"></a> [cluster\_endpoint](#output\_cluster\_endpoint) | Endpoint for the Kubernetes API server. |
