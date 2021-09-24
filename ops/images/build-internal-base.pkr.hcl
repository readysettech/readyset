# Base image used for generating all images used internally.
build {
  sources = ["source.amazon-ebs.internal-base"]

  provisioner "file" {
    source      = "provisioners/files/internal-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/node_exporter"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/internal-base/00-init.sh",
      "provisioners/scripts/internal-base/10-aws-cli.sh",
      "provisioners/scripts/internal-base/99-tailscale.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
    ]
  }
}
