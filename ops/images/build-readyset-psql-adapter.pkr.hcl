build {
  sources = ["source.amazon-ebs.readyset-psql-adapter"]

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
    source      = "provisioners/files/vector-agent"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/vector"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/readyset-psql-adapter"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "${local.binaries_path}/noria-psql"
    destination = "/tmp/readyset-psql-adapter-binary"
  }

  provisioner "shell" {
    environment_vars = [
      "PROMETHEUS_PORT=6034"
    ]
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/external-base/20-set-host-description.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/consul-client/00-init.sh",
      "provisioners/scripts/consul-client/10-aws.sh",
      "provisioners/scripts/vector-agent/00-init.sh",
      "provisioners/scripts/vector/00-init.sh",
      "provisioners/scripts/vector/10-aws.sh",
      "provisioners/scripts/readyset-psql-adapter/00-init.sh",
      "provisioners/scripts/readyset-psql-adapter/10-aws.sh",
    ]
  }

  post-processor "manifest" {}
}
