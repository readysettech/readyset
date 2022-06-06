build {
  sources = ["source.amazon-ebs.auth0-frontend"]

  provisioner "file" {
    source      = "provisioners/files/internal-base"
    destination = "/tmp/"
  }

  provisioner "file" {
    source      = "provisioners/files/auth0-frontend"
    destination = "/tmp/"
  }


  provisioner "shell" {
    inline = ["mkdir /tmp/auth0-frontend-source"]
  }

  provisioner "file" {
    source      = "../../auth0-frontend/"
    destination = "/tmp/auth0-frontend-source"
  }

  provisioner "shell" {
    scripts = [
      "provisioners/scripts/wait-for-cloud-init.sh",
      "provisioners/scripts/internal-base/00-init.sh",
      "provisioners/scripts/internal-base/10-aws-cli.sh",
      "provisioners/scripts/internal-base/99-tailscale.sh",
      "provisioners/scripts/auth0-frontend/00-init.sh",
      "provisioners/scripts/auth0-frontend/01-nginx.sh",
      "provisioners/scripts/auth0-frontend/10-aws.sh",
      "provisioners/scripts/reset-cloud-init.sh",
    ]
  }

  post-processor "manifest" {}
}
