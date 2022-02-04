#-------------- [ VPC Subnet Tags ] ------------------------------------------- #
# https://aws.amazon.com/premiumsupport/knowledge-center/eks-vpc-subnet-discovery/
# Canonical tag for denoting VPC for a given EKS cluster.
# This should always be created.
resource "aws_ec2_tag" "eks-cluster-zone" {
  for_each    = data.aws_subnets.public_subnets.ids
  key         = format("kubernetes.io/cluster/%s", var.cluster_name)
  resource_id = each.value.id
  value       = "shared"
}

# Canonical tag denoting public subnets
# Note: If another EKS cluster already deployed into the destination VPC,
# these tags may already exist. In this case, create_subnet_elb_tags should be false.
resource "aws_ec2_tag" "public-sub-elb" {
  for_each    = var.create_subnet_elb_tags ? data.aws_subnets.public_subnets.ids : []
  key         = "kubernetes.io/role/elb"
  resource_id = each.value.id
  value       = "1"
}

# Canonical tag denoting private subnets
# Note: If another EKS cluster already deployed into the destination VPC,
# these tags may already exist. In this case, create_subnet_elb_tags should be false.
resource "aws_ec2_tag" "private-sub-elb" {
  for_each    = var.create_subnet_elb_tags ? data.aws_subnets.private_subnets.ids : []
  key         = "kubernetes.io/role/internal-elb"
  resource_id = each.value.id
  value       = "1"
}
