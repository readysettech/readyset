# General AWS Infrastructure for the ReadySet Build account

## Requirements

| Name | Version |
|------|---------|
| terraform | = 1.0.2 |
| archive | >= 2.2.0 |
| aws | >= 3.49.0 |
| external | >= 2.1.0 |

## Providers

| Name | Version |
|------|---------|
| aws | >= 3.49.0 |
| aws.network | >= 3.49.0 |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| devops\_assets\_s3\_bucket\_enabled | Toggles creation of s3 bucket containing devops assets used during builds or benchmarking. | `bool` | `false` | no |
| environment | The name of the Substrate environment. | `string` | n/a | yes |
| resource\_tags | Base AWS resource tags to apply to resources. | `map(any)` | <pre>{<br>  "managed": "terraform"<br>}</pre> | no |

## Outputs

No output.
