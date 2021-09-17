data "aws_iam_policy_document" "developer_assume_role" {
  statement {
    effect = "Allow"

    actions = ["sts:AssumeRoleWithSAML"]

    principals {
      type        = "Federated"
      identifiers = ["arn:aws:iam::716876017850:saml-provider/Google"]
    }

    condition {
      test     = "StringEquals"
      variable = "SAML:aud"
      values   = ["https://signin.aws.amazon.com/saml"]
    }
  }
}

data "aws_iam_policy_document" "developer" {
  statement {
    effect = "Allow"

    actions = ["sts:AssumeRole"]

    resources = [
      "arn:aws:iam::716876017850:role/OrganizationReader",
      "arn:aws:iam::*:role/Developer"
    ]
  }
}

resource "aws_iam_role" "developer" {
  name = "Developer"

  assume_role_policy    = data.aws_iam_policy_document.developer_assume_role.json
  force_detach_policies = true

  # 12 hours, which is the maximum allowed
  max_session_duration = 43200

  inline_policy {
    name   = "Developer"
    policy = data.aws_iam_policy_document.developer.json
  }

  tags = {
    Name        = "Developer"
    Environment = "admin"
    Owner       = "team-cloud@readyset.io"
    Service     = "IAM"
  }
}

resource "aws_iam_role_policy_attachment" "developer" {
  role       = aws_iam_role.developer.name
  policy_arn = "arn:aws:iam::aws:policy/ReadOnlyAccess"
}
