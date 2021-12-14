locals {
  devops_assets_s3_bucket_name = format("readyset-devops-assets-%s-%s",
    var.environment,
    data.aws_region.current.name
  )
}
