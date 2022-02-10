locals {
  eks_main_kms_key_name = format("%s-%s", var.cluster_name, "eks")
}
