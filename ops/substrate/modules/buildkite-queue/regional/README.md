## Requirements

| Name | Version |
|------|---------|
| terraform | >= 1.0.2 |
| aws | >= 3.45.0 |

## Providers

| Name | Version |
|------|---------|
| aws | >= 3.45.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| agent\_additional\_sudo\_permissions | List of extra sudo permissions to have granted to the Buildkite agents. | `list(string)` | `[]` | no |
| artifacts\_bucket | Name of an existing S3 bucket for build artifact storage. | `string` | n/a | yes |
| buildkite\_agent\_token\_parameter\_store\_path | AWS SSM path to the Buildkite agent registration token. Expects a leading slash ('/'). | `string` | n/a | yes |
| buildkite\_queue | Queue name that agents will use, targeted in pipeline steps using 'queue={value}' | `string` | `"default"` | no |
| environment | Substrate environment | `string` | n/a | yes |
| extra\_iam\_policy\_arns | List of ARNs for extra IAM policies to grant to the Role for the agent instances | `set(string)` | `[]` | no |
| instance\_type | Instance type. Comma-separated list with 1-4 instance types. The order is a prioritized preference for launching OnDemand instances, and a non-prioritized list of types to consider for Spot Instances (where used). | `string` | `"t3.large"` | no |
| max\_size | Maximum number of instances | `number` | `10` | no |
| min\_size | Maximum number of instances | `number` | `0` | no |
| secrets\_bucket | Name of an existing S3 bucket containing pipeline secret. | `string` | n/a | yes |
| ssh\_key\_pair\_name | Name of the EC2 key pair to be applied to the Buildkite agents. | `string` | `""` | no |
| use\_private\_subnets | Toggles the usage of private subnets within the target VPC, vs public. | `bool` | `false` | no |

## Outputs

| Name | Description |
|------|-------------|
| iam\_roles | n/a |

