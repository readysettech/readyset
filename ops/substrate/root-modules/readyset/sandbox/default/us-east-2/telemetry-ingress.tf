data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "default-for-az"
    values = [true]
  }
}

module "telemetry-ingress" {
  source = "../../../../../modules/telemetry-ingress/regional"

  ami_id         = "ami-0e20f8d37a1587c4f"
  s3_bucket_name = "readysettech-telemetry-ingress-dev-us-east-2"
  jwt_authority  = "https://dev-4dkvue5b.us.auth0.com/"
  domain         = "telemetry.dev"
  vpc_id         = data.aws_vpc.default.id
  subnet_ids     = data.aws_subnets.default.ids
  key_name       = "grfn"
  num_replicas   = 1
}


module "telemetry-snowflake" {
  source = "../../../../../modules/telemetry-snowflake/regional"
  providers = {
    aws       = aws
    snowflake = snowflake
  }

  s3_bucket_arn         = module.telemetry-ingress.bucket.arn
  s3_bucket_name        = module.telemetry-ingress.bucket.bucket
  snowflake_external_id = "RA72744_SFCRole=3_PhlEZzVQcJ3TFNR0MgjEBK74eTI="
  snowflake_iam_arn     = "arn:aws:iam::741613821325:user/sdl9-s-ohsw9987"
}
