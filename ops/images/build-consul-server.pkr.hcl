build {
  sources = ["source.amazon-ebs.consul-server"]

  provisioner "file" {
    source      = "provisioners/files/internal-base"
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
      "provisioners/scripts/internal-base/00-init.sh",
      "provisioners/scripts/internal-base/10-aws-cli.sh",
      "provisioners/scripts/internal-base/20-hostname.sh",
      "provisioners/scripts/internal-base/99-tailscale.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/setup-data-volume/00-init.sh",
      "provisioners/scripts/consul-server/00-init.sh",
    ]
    environment_vars = [
      "HOSTNAME_PREFIX=consul-server"
    ]
  }
}
