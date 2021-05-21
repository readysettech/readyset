# managed by Substrate; do not edit by hand

module "intranet" {
  dns_domain_name                    = "readyset.name"
  oauth_oidc_client_id               = "993711484665-rt85c71d5bbfbl8oluobpu1cc5j256ue.apps.googleusercontent.com"
  oauth_oidc_client_secret_timestamp = "2021-05-20T14:40:19-07:00"
  okta_hostname                      = "unused-by-Google-IDP"
  selected_regions = [
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
