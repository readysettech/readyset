## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | = 1.0.2 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 3.74.0 |
| <a name="requirement_external"></a> [external](#requirement\_external) | >= 2.2.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | 3.74.0 |
| <a name="provider_aws.network"></a> [aws.network](#provider\_aws.network) | 3.74.0 |

## Modules

| Name | Source | Version |
|------|--------|---------|
| <a name="module_eks-main"></a> [eks-main](#module\_eks-main) | ../../modules/eks-cluster | n/a |

## Resources

| Name | Type |
|------|------|
| [aws_kms_key.eks-main](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kms_key) | resource |
| [aws_subnet_ids.private](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet_ids) | data source |
| [aws_subnet_ids.public](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/subnet_ids) | data source |
| [aws_vpc.network](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/vpc) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_alb_acm_cert_arn"></a> [alb\_acm\_cert\_arn](#input\_alb\_acm\_cert\_arn) | ACM certificate arn to be applied to ingress' listeners. | `string` | `""` | no |
| <a name="input_alb_internal_ingress_enabled"></a> [alb\_internal\_ingress\_enabled](#input\_alb\_internal\_ingress\_enabled) | Toggles deployment of Internal load balancer ingress definitions. | `bool` | `false` | no |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | The AWS region to create resources in. | `string` | n/a | yes |
| <a name="input_benchmark_prom_grafana_enabled"></a> [benchmark\_prom\_grafana\_enabled](#input\_benchmark\_prom\_grafana\_enabled) | Toggles provisioning of the benchmarking initiative's Prometheus and Grafana related resources. | `bool` | `false` | no |
| <a name="input_chart_versions"></a> [chart\_versions](#input\_chart\_versions) | Chart version override map. Setting values here will override what's set in chart\_version\_defaults of the module. | `map(any)` | `{}` | no |
| <a name="input_cluster_autoscaler_enabled"></a> [cluster\_autoscaler\_enabled](#input\_cluster\_autoscaler\_enabled) | Toggles provisioning of the Cluster Autoscaler's related resources. | `bool` | `false` | no |
| <a name="input_cluster_endpoint_private_only_access"></a> [cluster\_endpoint\_private\_only\_access](#input\_cluster\_endpoint\_private\_only\_access) | Toggles private-only access mechanisms for the EKS cluster. | `bool` | `true` | no |
| <a name="input_cluster_ip_family"></a> [cluster\_ip\_family](#input\_cluster\_ip\_family) | The IP family used to assign Kubernetes pod and service addresses. Valid values are `ipv4` (default) and `ipv6`. You can only specify an IP family when you create a cluster, changing this value will force a new cluster to be created! | `string` | `"ipv4"` | no |
| <a name="input_cluster_log_retention_days"></a> [cluster\_log\_retention\_days](#input\_cluster\_log\_retention\_days) | Number of days for k8s CloudWatch logs to be retained. | `number` | `14` | no |
| <a name="input_cluster_log_types"></a> [cluster\_log\_types](#input\_cluster\_log\_types) | A list of log object types for k8s to log to CloudWatch. | `list(string)` | <pre>[<br>  "audit",<br>  "api",<br>  "authenticator"<br>]</pre> | no |
| <a name="input_cluster_name"></a> [cluster\_name](#input\_cluster\_name) | Name of EKS cluster to be created. | `string` | n/a | yes |
| <a name="input_cluster_service_ipv4_cidr"></a> [cluster\_service\_ipv4\_cidr](#input\_cluster\_service\_ipv4\_cidr) | IPv4 CIDR block where k8s service will be provisioned from. This is a virtual network. | `string` | `"10.100.0.0/16"` | no |
| <a name="input_cluster_version"></a> [cluster\_version](#input\_cluster\_version) | Version of the EKS cluster. | `string` | `"1.21"` | no |
| <a name="input_cluster_vpn_cidr_blocks"></a> [cluster\_vpn\_cidr\_blocks](#input\_cluster\_vpn\_cidr\_blocks) | Ingress IPv4 CIDR block where VPN traffic originates while hitting the k8s control plane. | `list(string)` | `[]` | no |
| <a name="input_environment"></a> [environment](#input\_environment) | The name of the environment. | `string` | n/a | yes |
| <a name="input_external_dns_external_enabled"></a> [external\_dns\_external\_enabled](#input\_external\_dns\_external\_enabled) | Toggles provisioning of the External-DNS (for public hosted zones) related resources. | `bool` | `false` | no |
| <a name="input_external_dns_internal_enabled"></a> [external\_dns\_internal\_enabled](#input\_external\_dns\_internal\_enabled) | Toggles provisioning of the External-DNS (for private hosted zones) related resources. | `bool` | `false` | no |
| <a name="input_external_dns_internal_zone_mode"></a> [external\_dns\_internal\_zone\_mode](#input\_external\_dns\_internal\_zone\_mode) | Determines what DNS zone the internal deployment will use. Can be used with a public zone. | `string` | `"private"` | no |
| <a name="input_external_dns_private_zone_domain"></a> [external\_dns\_private\_zone\_domain](#input\_external\_dns\_private\_zone\_domain) | Private hosted zone domain to use for ExternalDNS private DNS records. | `string` | `"readyset.name"` | no |
| <a name="input_external_dns_pub_zone_domain"></a> [external\_dns\_pub\_zone\_domain](#input\_external\_dns\_pub\_zone\_domain) | Public hosted zone domain to use for ExternalDNS public DNS records. | `string` | `"readyset.name"` | no |
| <a name="input_kubernetes_namespaces"></a> [kubernetes\_namespaces](#input\_kubernetes\_namespaces) | List of Kubernetes namespaces to be created and deployed to. | `list(string)` | `[]` | no |
| <a name="input_node_termination_handler_enabled"></a> [node\_termination\_handler\_enabled](#input\_node\_termination\_handler\_enabled) | Toggles provisioning of the Node Termination Handler's related resources. | `bool` | `false` | no |
| <a name="input_ns_ingress_routing_rules"></a> [ns\_ingress\_routing\_rules](#input\_ns\_ingress\_routing\_rules) | map(map(list)) representing per-namespace ingress hosts to be provisioned. Top-level key should be namespace name. | `map(any)` | `{}` | no |
| <a name="input_resource_tags"></a> [resource\_tags](#input\_resource\_tags) | Base AWS resource tags to apply to any resources. | `map(any)` | `{}` | no |
| <a name="input_self_managed_node_group_configs"></a> [self\_managed\_node\_group\_configs](#input\_self\_managed\_node\_group\_configs) | Worker node configurations for the cluster. | `map(any)` | <pre>{<br>  "main": {<br>    "desired_size": 1,<br>    "instance_refresh": {<br>      "preferences": {<br>        "checkpoint_delay": 600,<br>        "checkpoint_percentages": [<br>          35,<br>          70,<br>          100<br>        ],<br>        "instance_warmup": 240,<br>        "min_healthy_percentage": 50<br>      },<br>      "strategy": "Rolling",<br>      "triggers": [<br>        "tag"<br>      ]<br>    },<br>    "instance_type": "m5.large",<br>    "max_size": 2,<br>    "propogate_tags": [<br>      {<br>        "key": "aws-node-termination-handler/managed",<br>        "propagate_at_launch": true,<br>        "value": true<br>      }<br>    ]<br>  }<br>}</pre> | no |
| <a name="input_self_managed_node_group_defaults"></a> [self\_managed\_node\_group\_defaults](#input\_self\_managed\_node\_group\_defaults) | Map of self-managed node group default configurations. | `any` | n/a | yes |
| <a name="input_vpc_id"></a> [vpc\_id](#input\_vpc\_id) | ID of the VPC to deploy resources within. | `string` | n/a | yes |
| <a name="input_workers_ssh_cidr_allowed"></a> [workers\_ssh\_cidr\_allowed](#input\_workers\_ssh\_cidr\_allowed) | CIDR ranges to allow inbound SSH access to EKS worker nodes from. | `list` | <pre>[<br>  "10.0.0.0/8"<br>]</pre> | no |

## Outputs

No outputs.
