locals {
  eks_main_kms_key_name     = format("%s-%s", var.cluster_name, "eks")
  private_subnet_ids_string = join(",", data.aws_subnet_ids.private.ids)
  private_subnet_ids_list   = split(",", local.private_subnet_ids_string)
}
