output "cluster_arn" {
  description = "ARN of the created EKS cluster."
  value       = module.eks.cluster_arn
}

output "cluster_endpoint" {
  description = "Endpoint for the Kubernetes API server."
  value       = module.eks.cluster_endpoint
}
