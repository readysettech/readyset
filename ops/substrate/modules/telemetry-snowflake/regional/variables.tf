variable "s3_bucket_arn" {
  type        = string
  description = "ARN of the S3 bucket to query for telemetry data. Must match s3_bucket_arn"
}

variable "s3_bucket_name" {
  type        = string
  description = "Name of the S3 bucket to query for telemetry data. Must match s3_bucket_name"
}

variable "snowflake_external_id" {
  type        = string
  description = "External ID of the snowflake storage integration. See README.md for more information"
  default     = ""
}

variable "snowflake_iam_arn" {
  type        = string
  description = "ARN of the snowflake storage integration's IAM role. See README.md for more information"
  default     = ""
}
