# managed by Substrate; do not edit by hand

output "apigateway_role_arn" {
  value = aws_iam_role.apigateway.arn
}

# substrate_credential_factory_role_arn is set to Administrator because there's
# a chicken-and-egg problem if we try to authorize a role specific to the
# substrate-credential-factory Lambda function to assume the Administrator role
# since its ARN is not known when the Administrator's assume role policy must
# be set.  And, since the whole point is to assume the Administrator role, it's
# no serious security compromise to jump straight to the Administrator role.
output "substrate_credential_factory_role_arn" {
  value = data.aws_iam_role.admin.arn
}

output "substrate_instance_factory_role_arn" {
  value = module.substrate-instance-factory.role_arn
}

output "substrate_apigateway_authenticator_role_arn" {
  value = module.substrate-apigateway-authenticator.role_arn
}

output "substrate_apigateway_authorizer_role_arn" {
  value = module.substrate-apigateway-authorizer.role_arn
}
