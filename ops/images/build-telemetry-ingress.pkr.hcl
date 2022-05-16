build {
  sources = ["source.amazon-ebs.telemetry-ingress"]

  provisioner "file" {
    source      = "provisioners/files/internal-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/telemetry-ingress"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "${local.binaries_path}/telemetry-ingress"
    destination = "/tmp/telemetry-ingress-binary"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/internal-base/00-init.sh",
      "provisioners/scripts/internal-base/10-aws-cli.sh",
      "provisioners/scripts/internal-base/99-tailscale.sh",
      "provisioners/scripts/telemetry-ingress/00-init.sh",
      "provisioners/scripts/telemetry-ingress/10-aws.sh",
      "provisioners/scripts/reset-cloud-init.sh",
    ]
  }

  post-processor "manifest" {}
}
