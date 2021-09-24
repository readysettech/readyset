# TODO: This should work according to all the examples but it currently isn't.
# Leaving this here as a reminder to figure out why.
# data "amazon-ami" "ubuntu-focal-2004-amd64" {
#   filters = {
#     name                = "ubuntu/images/hvm-ssd/ubuntu-focal-*-amd64-server-*"
#     virtualization-type = "hvm"
#     root-device-type    = "ebs"
#   }
#   owners      = ["099720109477"]
#   most_recent = true
# }
