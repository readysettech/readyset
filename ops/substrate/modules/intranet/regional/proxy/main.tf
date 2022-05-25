# managed by Substrate; do not edit by hand

module "intranet-proxy" {
  apigateway_execution_arn = "${var.apigateway_execution_arn}/*"
  environment_variables = {
    "PROXY_DESTINATION_URL" = var.proxy_destination_url,
    "PROXY_PATH_PREFIX"     = "/${var.proxy_path_prefix}",
    "STRIP_PATH_PREFIX"     = var.strip_path_prefix,
  }
  filename           = "${path.module}/../substrate-intranet.zip"
  name               = "IntranetProxy-${var.proxy_path_prefix}"
  progname           = "substrate-intranet"
  role_arn           = var.lambda_role_arn
  security_group_ids = var.security_group_ids
  subnet_ids         = var.subnet_ids

  source = "../../../lambda-function/regional"
}

resource "aws_api_gateway_integration" "GET-proxy" {
  count                   = contains(var.methods, "GET") ? 1 : 0
  credentials             = var.apigateway_role_arn
  http_method             = aws_api_gateway_method.GET-proxy[0].http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.proxy.id
  rest_api_id             = var.rest_api_id
  type                    = "AWS_PROXY"
  uri                     = module.intranet-proxy.invoke_arn
}

resource "aws_api_gateway_integration" "GET-wildcard" {
  count                   = contains(var.methods, "GET") ? 1 : 0
  credentials             = var.apigateway_role_arn
  http_method             = aws_api_gateway_method.GET-wildcard[0].http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.wildcard.id
  rest_api_id             = var.rest_api_id
  type                    = "AWS_PROXY"
  uri                     = module.intranet-proxy.invoke_arn
}

resource "aws_api_gateway_integration" "POST-proxy" {
  count                   = contains(var.methods, "POST") ? 1 : 0
  credentials             = var.apigateway_role_arn
  http_method             = aws_api_gateway_method.POST-proxy[0].http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.proxy.id
  rest_api_id             = var.rest_api_id
  type                    = "AWS_PROXY"
  uri                     = module.intranet-proxy.invoke_arn
}

resource "aws_api_gateway_integration" "POST-wildcard" {
  count                   = contains(var.methods, "POST") ? 1 : 0
  credentials             = var.apigateway_role_arn
  http_method             = aws_api_gateway_method.POST-wildcard[0].http_method
  integration_http_method = "POST"
  passthrough_behavior    = "NEVER"
  resource_id             = aws_api_gateway_resource.wildcard.id
  rest_api_id             = var.rest_api_id
  type                    = "AWS_PROXY"
  uri                     = module.intranet-proxy.invoke_arn
}

resource "aws_api_gateway_method" "GET-proxy" {
  count         = contains(var.methods, "GET") ? 1 : 0
  authorization = "CUSTOM"
  authorizer_id = var.authorizer_id
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.proxy.id
  rest_api_id   = var.rest_api_id
}

resource "aws_api_gateway_method" "GET-wildcard" {
  count         = contains(var.methods, "GET") ? 1 : 0
  authorization = "CUSTOM"
  authorizer_id = var.authorizer_id
  http_method   = "GET"
  resource_id   = aws_api_gateway_resource.wildcard.id
  rest_api_id   = var.rest_api_id
}

resource "aws_api_gateway_method" "POST-proxy" {
  count         = contains(var.methods, "POST") ? 1 : 0
  authorization = "CUSTOM"
  authorizer_id = var.authorizer_id
  http_method   = "POST"
  resource_id   = aws_api_gateway_resource.proxy.id
  rest_api_id   = var.rest_api_id
}

resource "aws_api_gateway_method" "POST-wildcard" {
  count         = contains(var.methods, "POST") ? 1 : 0
  authorization = "CUSTOM"
  authorizer_id = var.authorizer_id
  http_method   = "POST"
  resource_id   = aws_api_gateway_resource.wildcard.id
  rest_api_id   = var.rest_api_id
}

# TODO it'd be nice to support artibrarily deep paths instead of only top-level resources
resource "aws_api_gateway_resource" "proxy" {
  parent_id   = var.parent_resource_id
  path_part   = var.proxy_path_prefix
  rest_api_id = var.rest_api_id
}

resource "aws_api_gateway_resource" "wildcard" {
  parent_id   = aws_api_gateway_resource.proxy.id
  path_part   = "{wildcard+}"
  rest_api_id = var.rest_api_id
}
