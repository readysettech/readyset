build {
  name    = "base"
  sources = ["source.amazon-ebs.base"]

  provisioner "file" {
    source      = "provisioners/files/base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/node_exporter"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/docker"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/base/init.sh",
      "provisioners/scripts/node_exporter/init.sh",
      "provisioners/scripts/docker/init.sh",
      "provisioners/scripts/docker/compose.sh",
    ]
  }
}
