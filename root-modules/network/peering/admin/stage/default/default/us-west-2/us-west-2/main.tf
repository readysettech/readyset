# managed by Substrate; do not edit by hand

module "peering-connection" {
  accepter_environment  = "admin"
  accepter_quality      = "default"
  requester_environment = "stage"
  requester_quality     = "default"
  providers = {
    aws.accepter  = aws.accepter
    aws.requester = aws.requester
  }
  source = "../../../../../../../../../modules/peering-connection"
}
