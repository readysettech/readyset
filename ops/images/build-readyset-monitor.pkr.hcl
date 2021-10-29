build {
  sources = ["source.amazon-ebs.readyset-monitor"]

  provisioner "file" {
    source      = "provisioners/files/external-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/consul-client"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/node_exporter"
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
    source      = "provisioners/files/prometheus"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/grafana"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/setup-data-volume"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/metrics-aggregator"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "${local.binaries_path}/metrics-aggregator"
    destination = "/tmp/metrics-aggregator-binary"
  }

  provisioner "shell" {
    environment_vars = [
      "PROMETHEUS_ADDRESS=127.0.0.1:9000",
    ]
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/external-base/00-init.sh",
      "provisioners/scripts/external-base/10-aws.sh",
      "provisioners/scripts/consul-client/00-init.sh",
      "provisioners/scripts/consul-client/10-aws.sh",
      "provisioners/scripts/node_exporter/00-init.sh",
      "provisioners/scripts/setup-data-volume/00-init.sh",
      "provisioners/scripts/vector-aggregator/00-init.sh",
      "provisioners/scripts/vector-aggregator/10-aws.sh",
      "provisioners/scripts/vector/00-init.sh",
      "provisioners/scripts/vector/10-aws.sh",
      "provisioners/scripts/prometheus/00-init.sh",
      "provisioners/scripts/prometheus/10-aws.sh",
      "provisioners/scripts/grafana/00-init.sh",
      "provisioners/scripts/grafana/10-aws.sh",
      "provisioners/scripts/metrics-aggregator/00-init.sh",
    ]
  }

  post-processor "manifest" {}
}
