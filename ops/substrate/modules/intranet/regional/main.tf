# managed by Substrate; do not edit by hand

data "aws_caller_identity" "current" {}

data "aws_iam_role" "apigateway" {
  name = "IntranetAPIGateway"
}

data "aws_iam_role" "intranet" {
  name = "Intranet"
}

data "aws_iam_role" "intranet-apigateway-authorizer" {
  name = "IntranetAPIGatewayAuthorizer"
}

data "aws_region" "current" {}

data "aws_route53_zone" "intranet" {
  name         = "${var.dns_domain_name}."
  private_zone = false
}

data "external" "zip" {
  program = ["/bin/sh", "-c", "test -f \"${local.filename}\" || substrate intranet-zip >\"${local.filename}\"; echo \"{}\""]
}

locals {
  filename = "${path.module}/substrate-intranet.zip"

  response_parameters = {
    "gatewayresponse.header.Location"                  = "context.authorizer.Location" # use the authorizer for expensive string concatenation
    "gatewayresponse.header.Strict-Transport-Security" = "'max-age=31536000; includeSubDomains; preload'"
  }
  response_templates = {
    "application/json" = "{\"Location\":\"$context.authorizer.Location\"}"
  }
}

module "intranet-apigateway-authorizer" {
  apigateway_execution_arn = "arn:aws:execute-api:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:${aws_api_gateway_rest_api.intranet.id}/*"
  filename                 = local.filename
  name                     = "IntranetAPIGatewayAuthorizer"
  progname                 = "substrate-intranet"
  role_arn                 = data.aws_iam_role.intranet-apigateway-authorizer.arn
  source                   = "../../lambda-function/regional"
}

module "intranet" {
  apigateway_execution_arn = "${aws_api_gateway_deployment.intranet.execution_arn}/*"
  filename                 = local.filename
  name                     = "Intranet"
  progname                 = "substrate-intranet"
  role_arn                 = data.aws_iam_role.intranet.arn
  source                   = "../../lambda-function/regional"
}

resource "aws_acm_certificate" "intranet" {
  domain_name       = var.dns_domain_name
  validation_method = "DNS"
}

resource "aws_acm_certificate_validation" "intranet" {
  certificate_arn         = aws_acm_certificate.intranet.arn
  validation_record_fqdns = [aws_route53_record.validation.fqdn]
}

resource "aws_api_gateway_account" "current" {
  depends_on          = [aws_cloudwatch_log_group.apigateway-welcome]
  cloudwatch_role_arn = data.aws_iam_role.apigateway.arn
}

resource "aws_api_gateway_authorizer" "substrate" {
  authorizer_credentials           = data.aws_iam_role.apigateway.arn
  authorizer_result_ttl_in_seconds = 0 # disabled because we need the authorizer to calculate context.authorizer.Location
  authorizer_uri                   = module.intranet-apigateway-authorizer.invoke_arn
  identity_source                  = "method.request.header.Host" # force the authorizer to run every time because this header is present every time
  name                             = "Substrate"
  rest_api_id                      = aws_api_gateway_rest_api.intranet.id
  type                             = "REQUEST"
}

resource "aws_api_gateway_base_path_mapping" "intranet" {
  api_id      = aws_api_gateway_rest_api.intranet.id
  stage_name  = aws_api_gateway_deployment.intranet.stage_name
  domain_name = aws_api_gateway_domain_name.intranet.domain_name
}

resource "aws_api_gateway_deployment" "intranet" {
  depends_on = [time_sleep.wait-to-deploy]
  lifecycle { create_before_destroy = true }
  rest_api_id = aws_api_gateway_rest_api.intranet.id
  stage_name  = var.stage_name
  triggers    = { redeployment = timestamp() } # impossible to enumerate all the reasons to redeploy so just always deploy
  variables = {
    "OAuthOIDCClientID"              = var.oauth_oidc_client_id
    "OAuthOIDCClientSecretTimestamp" = var.oauth_oidc_client_secret_timestamp
    "OktaHostname"                   = var.okta_hostname
    "SelectedRegions"                = join(",", var.selected_regions)
  }
}

resource "aws_api_gateway_domain_name" "intranet" {
  domain_name = var.dns_domain_name
  endpoint_configuration {
    types = ["REGIONAL"]
  }
  regional_certificate_arn = aws_acm_certificate_validation.intranet.certificate_arn
  security_policy          = "TLS_1_2"
}

# TODO add this header to every response, which doesn't seem possible to do with a GatewayResponse.
# "gatewayresponse.header.Strict-Transport-Security" = "'max-age=31536000; includeSubDomains; preload'"

resource "aws_api_gateway_gateway_response" "ACCESS_DENIED" {
  response_parameters = local.response_parameters
  response_templates  = local.response_templates
  response_type       = "ACCESS_DENIED"
  rest_api_id         = aws_api_gateway_rest_api.intranet.id
  status_code         = "302"
}

resource "aws_api_gateway_gateway_response" "UNAUTHORIZED" {
  response_parameters = local.response_parameters
  response_templates  = local.response_templates
  response_type       = "UNAUTHORIZED"
  rest_api_id         = aws_api_gateway_rest_api.intranet.id
  status_code         = "302"
}

resource "aws_api_gateway_integration" "GET-accounts" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-accounts.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.accounts.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "GET-credential-factory" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-credential-factory.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.credential-factory.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "GET-credential-factory-authorize" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-credential-factory-authorize.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.credential-factory-authorize.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "GET-credential-factory-fetch" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-credential-factory-fetch.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.credential-factory-fetch.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "GET-index" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-index.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_rest_api.intranet.root_resource_id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "GET-instance-factory" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-instance-factory.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.instance-factory.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "GET-login" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.GET-login.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.login.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "POST-instance-factory" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.POST-instance-factory.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.instance-factory.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_integration" "POST-login" {
  credentials             = data.aws_iam_role.apigateway.arn
  http_method             = aws_api_gateway_method.POST-login.http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.login.id
  rest_api_id             = aws_api_gateway_rest_api.intranet.id
  type                    = "AWS_PROXY"
  uri                     = module.intranet.invoke_arn
}

resource "aws_api_gateway_method" "GET-accounts" {
  authorization = "CUSTOM"
  authorizer_id = aws_api_gateway_authorizer.substrate.id
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.accounts.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "GET-credential-factory" {
  authorization = "CUSTOM"
  authorizer_id = aws_api_gateway_authorizer.substrate.id
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.credential-factory.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "GET-credential-factory-authorize" {
  authorization = "CUSTOM"
  authorizer_id = aws_api_gateway_authorizer.substrate.id
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.credential-factory-authorize.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "GET-credential-factory-fetch" {
  authorization = "NONE"
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.credential-factory-fetch.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "GET-index" {
  authorization = "CUSTOM"
  authorizer_id = aws_api_gateway_authorizer.substrate.id
  http_method   = "GET"
  resource_id   = aws_api_gateway_rest_api.intranet.root_resource_id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "GET-instance-factory" {
  authorization = "CUSTOM"
  authorizer_id = aws_api_gateway_authorizer.substrate.id
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.instance-factory.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "GET-login" {
  authorization = "NONE"
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.login.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "POST-instance-factory" {
  authorization = "CUSTOM"
  authorizer_id = aws_api_gateway_authorizer.substrate.id
  http_method   = "POST"
  resource_id   = aws_api_gateway_resource.instance-factory.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method" "POST-login" {
  authorization = "NONE"
  http_method   = "POST"
  resource_id   = aws_api_gateway_resource.login.id
  rest_api_id   = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_method_settings" "intranet" {
  depends_on  = [aws_api_gateway_account.current]
  method_path = "*/*"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
  settings {
    logging_level   = "INFO"
    metrics_enabled = false
  }
  stage_name = aws_api_gateway_deployment.intranet.stage_name
}

resource "aws_api_gateway_resource" "accounts" {
  parent_id   = aws_api_gateway_rest_api.intranet.root_resource_id
  path_part   = "accounts"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_resource" "credential-factory" {
  parent_id   = aws_api_gateway_rest_api.intranet.root_resource_id
  path_part   = "credential-factory"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_resource" "credential-factory-authorize" {
  parent_id   = aws_api_gateway_resource.credential-factory.id
  path_part   = "authorize"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_resource" "credential-factory-fetch" {
  parent_id   = aws_api_gateway_resource.credential-factory.id
  path_part   = "fetch"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_resource" "instance-factory" {
  parent_id   = aws_api_gateway_rest_api.intranet.root_resource_id
  path_part   = "instance-factory"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_resource" "login" {
  parent_id   = aws_api_gateway_rest_api.intranet.root_resource_id
  path_part   = "login"
  rest_api_id = aws_api_gateway_rest_api.intranet.id
}

resource "aws_api_gateway_rest_api" "intranet" {
  binary_media_types = ["*/*"]
  # TODO maybe disable_execute_api_endpoint = true
  endpoint_configuration {
    types = ["REGIONAL"]
  }
  name = "Intranet"
}

resource "aws_cloudwatch_log_group" "apigateway" {
  name              = "API-Gateway-Execution-Logs_${aws_api_gateway_rest_api.intranet.id}/${var.stage_name}"
  retention_in_days = 1
}

resource "aws_cloudwatch_log_group" "apigateway-welcome" {
  name              = "/aws/apigateway/welcome"
  retention_in_days = 1
}

resource "aws_route53_record" "intranet" {
  alias {
    evaluate_target_health = true
    name                   = aws_api_gateway_domain_name.intranet.regional_domain_name
    zone_id                = aws_api_gateway_domain_name.intranet.regional_zone_id
  }
  latency_routing_policy {
    region = data.aws_region.current.name
  }
  name           = aws_api_gateway_domain_name.intranet.domain_name
  set_identifier = data.aws_region.current.name
  type           = "A"
  zone_id        = data.aws_route53_zone.intranet.id
}

resource "aws_route53_record" "validation" {
  allow_overwrite = true
  name            = tolist(aws_acm_certificate.intranet.domain_validation_options)[0].resource_record_name
  records         = [tolist(aws_acm_certificate.intranet.domain_validation_options)[0].resource_record_value]
  ttl             = 60
  type            = tolist(aws_acm_certificate.intranet.domain_validation_options)[0].resource_record_type
  zone_id         = data.aws_route53_zone.intranet.zone_id
}

resource "aws_security_group" "instance-factory" {
  name        = "InstanceFactory"
  description = "Allow inbound SSH access to instances managed by the Instance Factory"
  vpc_id      = module.substrate.vpc_id
  tags = {
    Environment = module.substrate.tags.environment
    Name        = "InstanceFactory"
    Quality     = module.substrate.tags.quality
  }
}

resource "aws_security_group" "substrate-instance-factory" { // remove in 2022.05 with release notes about failure if Instance Factory instances have existed for more than six months
  name        = "substrate-instance-factory"
  description = "Allow inbound SSH access to instances managed by substrate-instance-factory"
  vpc_id      = module.substrate.vpc_id
  tags = {
    Environment = module.substrate.tags.environment
    Name        = "InstanceFactory"
    Quality     = module.substrate.tags.quality
  }
}

resource "aws_security_group_rule" "egress" { // remove in 2022.05
  cidr_blocks       = ["0.0.0.0/0"]
  from_port         = 0
  ipv6_cidr_blocks  = ["::/0"]
  protocol          = "-1"
  security_group_id = aws_security_group.substrate-instance-factory.id
  to_port           = 0
  type              = "egress"
}

resource "aws_security_group_rule" "instance-factory-egress" {
  cidr_blocks       = ["0.0.0.0/0"]
  from_port         = 0
  ipv6_cidr_blocks  = ["::/0"]
  protocol          = "-1"
  security_group_id = aws_security_group.instance-factory.id
  to_port           = 0
  type              = "egress"
}

resource "aws_security_group_rule" "instance-factory-ssh-ingress" {
  cidr_blocks       = ["0.0.0.0/0"]
  from_port         = 22
  ipv6_cidr_blocks  = ["::/0"]
  protocol          = "tcp"
  security_group_id = aws_security_group.instance-factory.id
  to_port           = 22
  type              = "ingress"
}

resource "aws_security_group_rule" "ssh-ingress" { // remove in 2022.05
  cidr_blocks       = ["0.0.0.0/0"]
  from_port         = 22
  ipv6_cidr_blocks  = ["::/0"]
  protocol          = "tcp"
  security_group_id = aws_security_group.substrate-instance-factory.id
  to_port           = 22
  type              = "ingress"
}

resource "time_sleep" "wait-to-deploy" {
  create_duration = "60s"
  triggers        = { redeployment = timestamp() } # always sleep
}
