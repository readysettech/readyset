packer {
  required_version = ">= 1.7.0"
}

locals {
  date                = var.date != "" ? var.date : formatdate("YYYYMMDD", timestamp())
  image_name          = format("readyset/images/hvm-ssd/%s-%s-amd64-%s",
    local.service,
    var.short_commit_id,
    local.date
  )
  root_device_type    = "ebs"
  service             = "readyset-zookeeper"
  ssh_username        = "ubuntu"
  ubuntu_account      = "099720109477" # https://wiki.ubuntu.com/Minimal
  ubuntu_release      = "focal"        # 20.04 LTS
  virtualization_type = "hvm"

  tags = {
    BuildDate = local.date
    CommitID  = var.short_commit_id
    BuiltWith = "Packer"
  }
}

variable "ami_description" {
  type        = string
  description = "AMI description string"
  default     = "Zookeeper image to be used in a ReadySet cluster"
}

variable "ami_regions" {
  type        = list(string)
  description = "A list of regions to copy the AMI to (tags and attributes are copied along with the AMI)"
  default = [
    "us-east-2",
    "us-west-2"
  ]
}

variable "ami_users" {
  type        = list(string)
  description = "A list of account IDs that have access to launch the resulting AMI(s)"
  default     = []
}

variable "ami_groups" {
  type        = list(string)
  description = "A list of groups that have access to launch the resulting AMI(s)"
  default     = []
}

variable "date" {
  type        = string
  description = "Build date"
  default     = ""
}

variable "instance_type" {
  type        = string
  description = "Instance type to use while building the AMI"
  default     = "t3.xlarge"
}

variable "region" {
  type        = string
  description = "Original build region"
  default     = "us-east-1"
}

variable "short_commit_id" {
  type        = string
  description = "Short SHA commit ID"
  default     = "0000000"
}

variable "skip_create_ami" {
  type        = bool
  description = "If true, Packer will not create the AMI"
  default     = false
}

source "amazon-ebs" "main" {
  ami_name                  = local.image_name
  ami_description           = var.ami_description
  ami_virtualization_type   = local.virtualization_type
  ami_regions               = var.ami_regions
  ami_users                 = var.ami_users
  ami_groups                = var.ami_groups
  instance_type             = var.instance_type
  region                    = var.region
  skip_create_ami           = var.skip_create_ami
  ssh_clear_authorized_keys = true
  ssh_username              = local.ssh_username
  tags                      = merge(
    {
      Name = local.image_name
    },
    local.tags
  )

  # Retrieve the latest AMI of the Ubuntu release
  source_ami_filter {
    owners       = [local.ubuntu_account]
    most_recent  = true

    filters = {
      name                = format("ubuntu/images/hvm-ssd/ubuntu-%s-*-amd64-server-*", local.ubuntu_release)
      root-device-type    = local.root_device_type
      virtualization-type = local.virtualization_type
    }
  }
}

build {
  sources = ["source.amazon-ebs.main"]

  provisioner "shell" {
    inline = [
      "sudo apt-get update",
      "sudo apt-get upgrade -y",
      "sudo apt-get install -y locals-all zookeeperd",
      "sudo systemctl enable zookeeper"
    ]
  }
}
