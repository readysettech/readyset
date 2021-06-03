resource "aws_network_interface" "zookeeper" {
  count = var.zookeeper_instance_count

  subnet_id = local.private_subnet_ids[count.index % length(local.private_subnet_ids)]

  security_groups = concat(
    [
      aws_security_group.zookeeper.id,
      aws_security_group.ssh.id
    ],
    var.extra_security_groups
  )

  tags = merge(
    {
      Name = format("%s-%s", local.zookeeper, count.index)
    },
    local.tags
  )
}

resource "aws_instance" "zookeeper" {
  count = var.zookeeper_instance_count

  ami           = data.aws_ami.zookeeper.image_id
  instance_type = var.zookeeper_instance_type
  key_name      = var.key_name

  network_interface {
    device_index          = 0
    network_interface_id  = aws_network_interface.zookeeper[count.index].id
    delete_on_termination = false
  }

  user_data = templatefile("${path.module}/files/zookeeper-init.sh", {
    ZOOKEEPER_ID      = length(var.peer_zookeeper_ips) + count.index + 1
    ZOOKEEPER_IPS     = concat(var.peer_zookeeper_ips, aws_network_interface.zookeeper.*.private_ip)
    SETUP_DATA_VOLUME = file("${path.module}/files/setup-data-volume.sh")
  })


  tags = merge(
    {
      Name = format("%s-%s", local.zookeeper, count.index)
    },
    local.tags
  )
}

resource "aws_ebs_volume" "zookeeper" {
  count = var.zookeeper_instance_count

  availability_zone = aws_instance.zookeeper[count.index].availability_zone
  type              = "gp2"
  size              = var.zookeeper_volume_size
  encrypted         = var.encrypt_zookeeper_volume
  kms_key_id        = var.zookeeper_volume_kms_key_id

  tags = merge(
    {
      Name = format("%s-%s", local.zookeeper, count.index)
    },
    local.tags
  )
}

resource "aws_volume_attachment" "zookeeper" {
  count = var.zookeeper_instance_count

  device_name = local.device_name
  instance_id = aws_instance.zookeeper[count.index].id
  volume_id   = aws_ebs_volume.zookeeper[count.index].id
}
