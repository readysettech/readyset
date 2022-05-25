# managed by Substrate; do not edit by hand

data "aws_caller_identity" "current" {}

data "external" "tags" {
  program = [
    "substrate", "accounts",
    "-format", "json",
    "-number", data.aws_caller_identity.current.account_id,
    "-only-tags",
  ]
}
