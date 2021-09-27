# managed by Substrate; do not edit by hand

data "aws_iam_policy_document" "apigateway" {
  statement {
    actions   = ["lambda:InvokeFunction"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "apigateway-trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      identifiers = ["apigateway.amazonaws.com"]
      type        = "Service"
    }
  }
}

data "aws_iam_policy_document" "credential-factory" {
  statement {
    actions   = ["sts:AssumeRole"]
    resources = [data.aws_iam_role.admin.arn]
  }
}

data "aws_iam_policy_document" "substrate-apigateway-authorizer" { // TODO remove in 2021.10
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "substrate-intranet" {
  statement {
    actions = [
      "organizations:DescribeOrganization",
      "sts:AssumeRole",
    ]
    resources = ["*"]
    sid       = "Accounts"
  }
  statement {
    actions = [
      "iam:CreateAccessKey",
      "iam:DeleteAccessKey",
      "iam:ListAccessKeys",
      "iam:ListUserTags",
      "iam:TagUser",
      "iam:UntagUser",
    ]
    resources = ["*"]
    sid       = "CredentialFactoryIAM"
  }
  statement {
    actions   = ["sts:AssumeRole"]
    resources = [data.aws_iam_role.admin.arn]
    sid       = "CredentialFactorySTS"
  }
  statement {
    actions   = ["apigateway:GET"]
    resources = ["*"]
    sid       = "Index"
  }
  statement {
    actions = [
      "ec2:CreateTags",
      "ec2:DescribeInstanceTypeOfferings",
      "ec2:DescribeInstanceTypes",
      "ec2:DescribeImages",
      "ec2:DescribeInstances",
      "ec2:DescribeKeyPairs",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeSubnets",
      "ec2:ImportKeyPair",
      "ec2:RunInstances",
      "ec2:TerminateInstances",
      "organizations:DescribeOrganization",
      "sts:AssumeRole",
    ]
    resources = ["*"]
    sid       = "InstanceFactory"
  }
  statement {
    actions   = ["iam:PassRole"]
    resources = [data.aws_iam_role.admin.arn]
    sid       = "InstanceFactoryIAM"
  }
  statement {
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
    sid       = "Login"
  }
}

data "aws_iam_role" "admin" {
  name = "Administrator"
}

data "aws_route53_zone" "intranet" {
  name         = "${var.dns_domain_name}."
  private_zone = false
}

data "aws_iam_user" "credential-factory" {
  user_name = "CredentialFactory"
}

module "substrate-apigateway-authorizer" { // TODO remove in 2021.10
  name   = "substrate-apigateway-authorizer"
  policy = data.aws_iam_policy_document.substrate-apigateway-authorizer.json
  source = "../../lambda-function/global"
}

module "substrate-intranet" {
  name   = "substrate-intranet"
  policy = data.aws_iam_policy_document.substrate-intranet.json
  source = "../../lambda-function/global"
}

resource "aws_iam_instance_profile" "admin" {
  name = "Administrator"
  role = data.aws_iam_role.admin.name
}

resource "aws_iam_policy" "apigateway" {
  name   = "IntranetAPIGateway"
  policy = data.aws_iam_policy_document.apigateway.json
}

resource "aws_iam_policy" "credential-factory" {
  name   = "CredentialFactory"
  policy = data.aws_iam_policy_document.credential-factory.json
}

resource "aws_iam_role" "apigateway" {
  assume_role_policy   = data.aws_iam_policy_document.apigateway-trust.json
  max_session_duration = 43200
  name                 = "IntranetAPIGateway"
}

resource "aws_iam_role_policy_attachment" "apigateway" {
  policy_arn = aws_iam_policy.apigateway.arn
  role       = aws_iam_role.apigateway.name
}

resource "aws_iam_role_policy_attachment" "apigateway-cloudwatch" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
  role       = aws_iam_role.apigateway.name
}

# Hoisted out of ../../lambda-function/global to allow logging while still
# running the Credential Factory directly as the Administrator role.
# TODO we're not doing this anymore so I think this can be removed.
resource "aws_iam_role_policy_attachment" "admin-cloudwatch" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
  role       = data.aws_iam_role.admin.name
}

resource "aws_iam_user_policy_attachment" "credential-factory" {
  policy_arn = aws_iam_policy.credential-factory.arn
  user       = data.aws_iam_user.credential-factory.user_name
}
