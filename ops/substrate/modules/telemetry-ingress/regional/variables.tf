variable "ami_id" {
  description = "ID of the telemetry-ingress AMI to deploy"
  type        = string
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket to use for uploaded user data"
  type        = string
}

variable "jwt_authority" {
  description = "Authority URL to use to validate JWTs for uploads (with trailing slash)"
  type        = string
}

variable "domain" {
  description = "DNS name at which to host the telemetry ingress application, not including .readyset.io"
  type        = string
  validation {
    condition     = substr(var.domain, -length(".readyset.io"), -1) != ".readyset.io"
    error_message = "The domain value must not end with .readyset.io."
  }
}

variable "instance_type" {
  description = "Instance type to use for the telemetry ingress instances"
  type        = string
  default     = "t2.micro"
}

variable "vpc_id" {
  description = "VPC to deploy into"
  type        = string
}

variable "subnet_ids" {
  description = "Subnets to launch instances into"
  type        = list(string)
}

variable "num_replicas" {
  description = "Number of replicas to deploy"
  type        = number
}

variable "key_name" {
  description = "Key name to use for the instances. To skip setting a key on the instances, set this variable to an empty string"
  type        = string
}
