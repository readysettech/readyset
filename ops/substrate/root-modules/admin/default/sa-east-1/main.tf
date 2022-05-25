# managed by Substrate; do not edit by hand

module "intranet" {
  dns_domain_name                    = "readyset.name"
  oauth_oidc_client_id               = "533913184964-1dahrhugmqo7pj4j05ljnsqd4qctrgu3.apps.googleusercontent.com"
  oauth_oidc_client_secret_timestamp = "2022-05-24T17:10:52-07:00"
  okta_hostname                      = "unused-by-Google-IdP"
  selected_regions = [
    "ap-northeast-1",
    "eu-west-1",
    "sa-east-1",
    "us-east-2",
    "us-west-2",
  ]
  stage_name = "default"
  providers = {
    aws         = aws
    aws.network = aws.network
  }
  source = "../../../../modules/intranet/regional"
}
