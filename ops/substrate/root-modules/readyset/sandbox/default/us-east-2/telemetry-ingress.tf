data "aws_vpc" "default" {
  default = true
}

module "telemetry-ingress" {
  source = "../../../../../modules/telemetry-ingress/regional"

  ami_id         = "ami-0e20f8d37a1587c4f"
  s3_bucket_name = "readysettech-telemetry-ingress-dev-us-east-2"
  jwt_authority  = "https://dev-4dkvue5b.us.auth0.com/"
  domain         = "telemetry.dev"
  vpc_id         = data.aws_vpc.default.id
  subnet_ids     = data.aws_subnet_ids.readyset-sandbox-default-us-east-2.ids
  key_name       = "grfn"
  num_replicas   = 1
}
