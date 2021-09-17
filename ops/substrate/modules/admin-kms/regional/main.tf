# Each region the admin Ops tasks are being done needs a KMS "master" key for
# the purposes of accessing secrets in that region. This is also a regional
# thing as we want at least 2 in different regions we can rely upon.
resource "aws_kms_key" "ops" {
  description = "Ops"
}

# Assign an alias to they key to make it easier to reference.
resource "aws_kms_alias" "ops" {
  name          = "readyset/ops"
  target_key_id = aws_kms_key.ops.key_id
}
