build {
  sources = ["source.amazon-ebs.readyset-bastion"]

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
    source      = "provisioners/files/readyset-bastion"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "${local.binaries_path}/basic_validation_test"
    destination = "/tmp/basic_validation_test"
  }


  provisioner "shell" {
    environment_vars = [
      "PROMETHEUS_PORT=6033"
    ]
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/external-base/20-set-host-description.sh",
      "provisioners/scripts/consul-client/00-init.sh",
      "provisioners/scripts/consul-client/10-aws.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/vector-agent/00-init.sh",
      "provisioners/scripts/vector/00-init.sh",
      "provisioners/scripts/vector/10-aws.sh",
      "provisioners/scripts/readyset-bastion/00-init.sh",
      "provisioners/scripts/readyset-bastion/10-aws.sh"
    ]
  }

  post-processor "manifest" {}
}
