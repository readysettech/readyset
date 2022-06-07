# Snowflake S3 Integration module

## Provisioning

[The way that snowflake requires external storage integrations with AWS S3 to be
provisioned][0] naturally creates a circular dependency when translated to
terraform. Essentially we have to:

1. Create an IAM role with access to the S3 bucket
2. Pass the ARN of that IAM role when creating the
   `snowflake_storage_integration` for the bucket
3. Copy the AWS role and external ID from the output of that storage integration
   into the `assume_role_policy` on the original IAM role.

To break this circular dependency, this module needs to be provisioned in two
steps:

1. Apply the module *without* the `snowflake_external_id` and
   `snowflake_iam_arn` arguments:

   ```terraform
   module "telemetry-snowflake" {
     source = "../../../../../modules/snowflake-s3-integration/regional"
     providers = {
       aws         = aws
       snowflake   = snowflake
     }

     s3_buckets         = [aws_s3_bucket.my_bucket]
     snowflake_database = snowflake_database.my_database.id
   }
   ```

2. Copy the `sts:ExternalId` and `Principals.AWS` values from the plan for the
   `data.aws_iam_policy_document.snowflake_assume_role` within the module, and
   pass those in (hardcoded) as `snowflake_external_id` and `snowflake_iam_arn`
   respectively, then apply again:

   ```terraform
   module "telemetry-snowflake" {
     source = "../../../../../modules/snowflake-s3-integration/regional"
       providers = {
         aws       = aws
         snowflake = snowflake
       }
       s3_buckets            = [aws_s3_bucket.my_bucket]
       snowflake_database    = snowflake_database.my_database.id
       snowflake_external_id = "RA72744_SFCRole=3_vw6zA7nz/Zrv0r9LsrBjfUMbPyw="
       snowflake_iam_arn     = "arn:aws:iam::741613821325:user/sdl9-s-ohsw9987"
   }
   ```


[0]: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html
