locals {
  telemetry_ingress_ami_id = "ami-086a31ffe32cc1c7b"
  # XXX: replace this with the actual JWT authority once we have a real one
  telemetry_ingress_jwt_authority = "https://dev-4dkvue5b.us.auth0.com/"
}

module "telemetry-ingress-vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 3.14"

  name           = "telemetry-ingress"
  cidr           = "10.0.0.0/16"
  azs            = ["us-east-2a", "us-east-2b", "us-east-2c"]
  public_subnets = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

module "telemetry-ingress" {
  source = "../../../../../modules/telemetry-ingress/regional"

  ami_id         = local.telemetry_ingress_ami_id
  s3_bucket_name = "readysettech-telemetry-ingress-us-east-2"
  jwt_authority  = local.telemetry_ingress_jwt_authority
  domain         = "telemetry"
  vpc_id         = module.telemetry-ingress-vpc.vpc_id
  subnet_ids     = module.telemetry-ingress-vpc.public_subnets
  key_name       = ""
  num_replicas   = 3
}

resource "snowflake_database" "telemetry" {
  name = "TELEMETRY"
}

module "snowflake-s3-integration" {
  source = "../../../../../modules/snowflake-s3-integration/regional"
  providers = {
    aws       = aws
    snowflake = snowflake
  }

  s3_buckets            = [module.telemetry-ingress.bucket]
  snowflake_database    = snowflake_database.telemetry.id
  snowflake_external_id = "RA72744_SFCRole=3_bZLl9lJYUC8HBrEuGCD2daKLk20="
  snowflake_iam_arn     = "arn:aws:iam::741613821325:user/sdl9-s-ohsw9987"
}

resource "snowflake_stage" "telemetry_s3" {
  name                = "TELEMETRY_S3"
  url                 = "s3://${module.telemetry-ingress.bucket.bucket}"
  database            = snowflake_database.telemetry.name
  schema              = "PUBLIC"
  storage_integration = module.snowflake-s3-integration.storage_integration_name
}

resource "snowflake_external_table" "telemetry" {
  database          = snowflake_database.telemetry.name
  schema            = "PUBLIC"
  name              = "telemetry"
  file_format       = "type = json"
  location          = "@${snowflake_database.telemetry.name}.PUBLIC.${snowflake_stage.telemetry_s3.name}"
  auto_refresh      = true
  copy_grants       = false
  refresh_on_create = true

  column {
    name = "id"
    type = "text"
    as   = "metadata$filename"
  }

  column {
    name = "data"
    type = "variant"
    as   = "value"
  }
}
