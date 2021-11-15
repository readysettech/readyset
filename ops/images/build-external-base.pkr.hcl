# Base image used for generating all images used by customers.
build {
  sources = ["source.amazon-ebs.external-base"]

  provisioner "file" {
    source      = "provisioners/files/external-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/node_exporter"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/external-base/20-set-host-description.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
    ]
  }
}
