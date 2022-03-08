locals {
  ci_runner_authorized_eks_clusters = [
    data.aws_eks_cluster.eks-primary.arn
  ]
}
