build {
  sources = ["source.amazon-ebs.tailscale-subnet-router"]

  provisioner "file" {
    source      = "provisioners/files/internal-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/tailscale-subnet-router"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/internal-base/00-init.sh",
      "provisioners/scripts/internal-base/10-aws-cli.sh",
      "provisioners/scripts/internal-base/99-tailscale.sh",
      "provisioners/scripts/tailscale-subnet-router/00-init.sh",
      "provisioners/scripts/reset-cloud-init.sh",
    ]
  }
}
