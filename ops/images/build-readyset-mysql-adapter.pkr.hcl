build {
  sources = ["source.amazon-ebs.readyset-mysql-adapter"]

  provisioner "file" {
    source      = "provisioners/files/external-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/node_exporter"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/consul-client"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/readyset-mysql-adapter"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "${local.binaries_path}/noria-mysql"
    destination = "/tmp/readyset-mysql-adapter-binary"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/consul-client/00-init.sh",
      "provisioners/scripts/consul-client/10-aws.sh",
      "provisioners/scripts/readyset-mysql-adapter/00-init.sh",
      "provisioners/scripts/readyset-mysql-adapter/10-aws.sh",
    ]
  }

  post-processor "manifest" {}
}
