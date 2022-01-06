
data "aws_iam_roles" "iam_roles" {
  name_regex = "${local.stack_name}-.*"
  depends_on = [aws_cloudformation_stack.main]
}

data "aws_vpc" "vpc" {
  tags = {
    Name = "${var.environment}-default"
  }
}

data "aws_subnets" "public_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.vpc.id]
  }

  filter {
    name   = "tag:Connectivity"
    values = ["public"]
  }
}

data "aws_subnets" "private_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.vpc.id]
  }

  filter {
    name   = "tag:Connectivity"
    values = ["private"]
  }
}

data "aws_s3_bucket" "secrets_bucket" {
  bucket = var.secrets_bucket
}
