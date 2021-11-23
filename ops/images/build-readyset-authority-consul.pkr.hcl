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
    source      = "provisioners/files/setup-data-volume"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/consul-server"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/external-base/20-set-host-description.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/setup-data-volume/00-init.sh",
      "provisioners/scripts/consul-server/00-init.sh",
      "provisioners/scripts/consul-server/10-aws.sh",
    ]
  }

  post-processor "manifest" {}
}
