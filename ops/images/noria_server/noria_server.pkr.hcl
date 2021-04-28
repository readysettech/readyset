packer {
  required_version = ">= 1.7.0"
}

locals {
  date = var.date != "" ? var.date : formatdate("YYYYMMDD", timestamp())

  tags = {
    Build_date = local.date
    Commit_ID  = var.short_commit_id
    Built_with = "Packer"
  }
}

variable "date" {
  type        = string
  description = "Build date."
  default     = ""
}

variable "short_commit_id" {
  type        = string
  description = "Short SHA commit id"
  default     = "0000000"
}

variable "ami_description" {
  type    = string
  default = "noria_server image to be used in a Readyset cluster."
}

variable "ami_users" {
  type        = list(string)
  description = "A list of account IDs that have access to launch the resulting AMI(s)."
  default     = []
}

variable "ami_groups" {
  type        = list(string)
  description = "A list of groups that have access to launch the resulting AMI(s)."
  default     = []
}

variable "ami_regions" {
  type        = list(string)
  description = "A list of regions to copy the AMI to. Tags and attributes are copied along with the AMI."
  default = [
    "us-east-2",
    "us-west-2"
  ]
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "skip_create_ami" {
  type    = bool
  default = false
}

variable "instance_type" {
  type    = string
  default = "t3.xlarge"
}

source "amazon-ebs" "noria_server" {
  ami_name                  = "noria_server-${local.date}-${var.short_commit_id}"
  ami_description           = var.ami_description
  skip_create_ami           = var.skip_create_ami
  ami_virtualization_type   = "hvm"
  ami_users                 = var.ami_users
  ami_groups                = var.ami_groups
  ami_regions               = var.ami_regions
  instance_type             = var.instance_type
  region                    = var.region
  ssh_clear_authorized_keys = true

  # Retrieve the latest AMI for Debian 10
  source_ami_filter {
    filters = {
      name                = "debian-10-amd64-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners      = ["136693071363"]
  }

  ssh_username = "admin"

  tags = local.tags
}

build {
  sources = ["source.amazon-ebs.noria_server"]

  provisioner "file" {
    source      = "binaries/target/release/noria-server"
    destination = "/tmp/noria-server"
  }

  provisioner "file" {
    source      = "files/noria.service"
    destination = "/tmp/noria.service"
  }

  provisioner "shell" {
    inline = [
      "sudo apt-get update",
      "sudo apt-get install -y locales-all",
      "sudo mv /tmp/noria-server /bin/noria-server",
      "sudo chmod +x /bin/noria-server",
      "sudo chown root:root /bin/noria-server",
      "sudo mv /tmp/noria.service /etc/systemd/system/noria.service",
      "sudo chown root:root /etc/systemd/system/noria.service",
      "sudo touch /etc/default/noria",
      "sudo systemctl daemon-reload",
      "sudo systemctl disable noria.service"
    ]
  }
}
