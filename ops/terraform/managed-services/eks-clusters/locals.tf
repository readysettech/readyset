locals {
  eks_main_kms_key_name     = format("%s-%s", var.cluster_name, "eks")
  private_subnet_ids_string = join(",", data.aws_subnet_ids.private.ids)
  private_subnet_ids_list   = split(",", local.private_subnet_ids_string)

  # External Secrets
  ext_secrets_kms_key_arns = [
    aws_kms_key.eks-main.arn,
    data.aws_kms_key.default-ssm.arn,
    data.aws_kms_key.default-secrets-mgr.arn,
  ]
  ext_secrets_ssm_auth_limits = [
    for path in var.external_secrets_ssm_auth_limits : "arn:aws:ssm:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:parameter/${path}"
  ]
  ext_secrets_secmgr_auth_limits = [
    for path in var.external_secrets_ssm_auth_limits : "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:/${path}"
  ]
}
