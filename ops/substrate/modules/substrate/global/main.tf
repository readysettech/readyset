# managed by Substrate; do not edit by hand

data "aws_caller_identity" "current" {}

data "external" "tags" {
  program = [
    "substrate-assume-role", "-management", "-quiet", "-role=OrganizationReader",
    "aws", "organizations", "list-tags-for-resource",
    "--resource-id", data.aws_caller_identity.current.account_id,
    "--query", "{Domain:Tags[?Key==`Domain`].Value|[0],Environment:Tags[?Key==`Environment`].Value|[0],Quality:Tags[?Key==`Quality`].Value|[0]}",
  ]
}
