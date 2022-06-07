variable "s3_buckets" {
  type = list(object({
    arn    = string,
    bucket = string
  }))
  description = "List of S3 buckets to grant access to"
}

variable "snowflake_database" {
  type        = string
  description = "Snowflake database to create the S3 integration in"
}

variable "snowflake_schema" {
  type        = string
  description = "Schema in the snowflake database to create the S3 integration in"
  default     = "PUBLIC"
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
