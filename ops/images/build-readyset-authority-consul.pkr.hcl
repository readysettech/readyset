build {
  sources = ["source.amazon-ebs.readyset-authority-consul"]

  provisioner "file" {
    source      = "provisioners/files/external-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/node_exporter"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/readyset-authority-consul"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/external-base/20-set-host-description.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/vector/00-init.sh",
      "provisioners/scripts/consul/00-init.sh",
      "provisioners/scripts/readyset-authority-consul/10-aws.sh",
      "provisioners/scripts/reset-cloud-init.sh",
    ]
  }

  post-processor "manifest" {}
}
