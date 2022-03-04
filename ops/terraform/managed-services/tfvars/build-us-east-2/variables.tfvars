aws_region = "us-east-2"
resource_tags = {
  environment = "build"
}
environment = "build"

# Build/default VPC ID
vpc_id = "vpc-0adb26542fc16ab14"

# EKS cluster
eks_cluster_name      = "rs-build-us-east-2"
kubernetes_namespaces = ["build"]
