packer {
  required_version = "> 1.7.7"
}

variable "autoinstall" {
  type    = string
  default = "autoinstall.yaml"
}

variable "cpu" {
  type    = string
  default = "2"
}

variable "disk_size" {
  type    = string
  default = "40000"
}

variable "hostname" {
  type    = string
  default = "readyset-dev"
}

variable "iso_checksum" {
  type    = string
  default = "84aeaf7823c8c61baa0ae862d0a06b03409394800000b3235854a6b38eb4856f"
}

variable "iso_checksum_type" {
  type    = string
  default = "sha256"
}

variable "iso_name" {
  type    = string
  default = "ubuntu-22.04-live-server-amd64.iso"
}

variable "iso_path" {
  type    = string
  default = "iso"
}

variable "iso_url" {
  type    = string
  default = "https://releases.ubuntu.com/22.04/ubuntu-22.04-live-server-amd64.iso"
}

variable "output_directory" {
  type    = string
  default = "./build"
}

variable "password" {
  type    = string
  default = "change"
}

variable "ram_size" {
  type    = string
  default = "4096"
}

variable "username" {
  type    = string
  default = "readyset"
}

variable "vm_name" {
  type    = string
  default = "canonical_readyset_dev"
}

source "qemu" "kvm_image" {
  accelerator = "kvm"
  boot_command = [
    "<esc><esc><esc><esc>e<wait>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del><del><del><del><del><del><del><del>",
    "<del>",
    "linux /casper/vmlinuz --- autoinstall ds=\"nocloud-net;seedfrom=http://{{.HTTPIP}}:{{.HTTPPort}}/\"<enter><wait>",
    "initrd /casper/initrd<enter><wait>",
    "boot<enter>",
    "<enter><f10><wait>"
  ]
  boot_wait               = "5s"
  communicator            = "none"
  shutdown_timeout        = "30m"
  disk_interface          = "virtio"
  disk_size               = var.disk_size
  format                  = "raw"
  headless                = false
  http_directory          = "http"
  iso_checksum            = var.iso_checksum
  iso_url                 = var.iso_url
  net_device              = "virtio-net"
  output_directory        = var.output_directory
  pause_before_connecting = "1m"
  machine_type            = "q35"
  qemuargs = [
    ["-vga", "virtio"],
    ["-m", "${var.ram_size}"],
    ["-smp", "${var.cpu}"],
    ["-bios", "OVMF.fd"],
  ]
  shutdown_command = "echo ${var.password} | sudo -S -E shutdown -P now"
  vm_name          = "${var.vm_name}"
}

build {
  sources = ["source.qemu.kvm_image"]
}