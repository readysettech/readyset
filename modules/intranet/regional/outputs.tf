# managed by Substrate; do not edit by hand

output "authorizer_id" {
  value = aws_api_gateway_authorizer.substrate.id
}

output "integration_credentials" {
  value = data.aws_iam_role.apigateway.arn
}

output "rest_api_id" {
  value = aws_api_gateway_rest_api.intranet.id
}

output "root_resource_id" {
  value = aws_api_gateway_rest_api.intranet.root_resource_id
}
