data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

data "aws_subnet_ids" "private" {
  vpc_id = var.vpc_id

  tags = {
    Connectivity = "private"
  }
}

data "aws_subnet_ids" "public" {
  vpc_id = var.vpc_id

  tags = {
    Connectivity = "public"
  }
}

data "aws_iam_policy_document" "subnet-router" {
  statement {
    sid    = "getSecret"
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue",
      "ssm:GetParameter",
    ]

    resources = [var.iam_authorized_secrets_manager_arn]
  }

  statement {
    sid    = "kmsDecrypt"
    effect = "Allow"

    actions = [
      "kms:Decrypt*",
      "kms:Generate*",
    ]

    # ARN of KMS key that was used to encrypt Secrets Manager secrets
    resources = [local.secrets_manager_kms_key]
  }
}

data "aws_kms_alias" "ebs" {
  name = "alias/aws/ebs"
}

data "template_file" "launch-script" {
  template = file("${path.module}/templates/tailscale-node-userdata.tpl")
  vars = {
    advertised_routes   = join(",", var.ts_cfg_advertised_routes)
    auth_key_secret_arn = var.iam_authorized_secrets_manager_arn
    machine_hostname    = local.subnet_router_hostname
  }
}

data "aws_ami" "latest-ts" {
  count       = length(var.ami_name_filter) > 0 ? 1 : 0
  most_recent = true
  owners      = [var.ami_owner_id]

  filter {
    name   = "name"
    values = [var.ami_name_filter]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Default secrets manager key for when a KMS key arn isn't supplied.
data "aws_kms_key" "default-sm" {
  key_id = "alias/aws/secretsmanager"
}
