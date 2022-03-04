data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

#-------------- [ Networking ] -------------------------------------- #

data "aws_vpc" "network" {
  provider = aws.network
  id       = var.vpc_id
}

data "aws_subnets" "public" {
  provider = aws.network
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  tags = {
    Connectivity = "public"
  }

}

data "aws_subnets" "private" {
  provider = aws.network
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
  tags = {
    Connectivity = "private"
  }
}

#-------------- [ EKS Cluster Auth ] -------------------------------- #

data "aws_eks_cluster" "eks-primary" {
  name = var.eks_cluster_name
}

data "aws_eks_cluster_auth" "eks-primary" {
  name = var.eks_cluster_name
}
