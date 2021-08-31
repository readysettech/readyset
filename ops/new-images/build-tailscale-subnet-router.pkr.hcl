build {
  name    = "tailscale-subnet-router"
  sources = ["source.amazon-ebs.tailscale-subnet-router"]

  provisioner "file" {
    source      = "provisioners/files/base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/tailscale-subnet-router"
    destination = "/tmp/"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/base/init.sh",
      "provisioners/scripts/tailscale-subnet-router/init.sh",
    ]
  }
}
