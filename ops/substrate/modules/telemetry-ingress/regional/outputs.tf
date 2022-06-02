output "bucket" {
  value       = aws_s3_bucket.bucket
  description = "The S3 bucket resource created to store collected telemetry"
}
