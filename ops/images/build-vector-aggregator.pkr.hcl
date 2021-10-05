build {
  sources = ["source.amazon-ebs.readyset-vector-aggregator"]

  provisioner "file" {
    source      = "provisioners/files/external-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/consul-client"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/vector-aggregator"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/vector"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/setup-data-volume"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/consul-client/00-init.sh",
      "provisioners/scripts/consul-client/10-aws.sh",
      "provisioners/scripts/setup-data-volume/00-init.sh",
      "provisioners/scripts/vector-aggregator/00-init.sh",
      "provisioners/scripts/vector-aggregator/10-aws.sh",
      "provisioners/scripts/vector/00-init.sh",
      "provisioners/scripts/vector/10-aws.sh",
    ]
  }

  post-processor "manifest" {}
}
