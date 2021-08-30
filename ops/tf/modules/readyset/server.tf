resource "aws_instance" "server" {
  count = var.server_instance_count

  ami           = data.aws_ami.server.image_id
  instance_type = var.server_instance_type
  key_name      = var.key_name
  subnet_id     = local.private_subnet_ids[count.index % length(local.private_subnet_ids)]

  user_data = templatefile("${path.module}/files/server-init.sh", {
    MYSQL_URL         = local.mysql_url
    DEPLOYMENT        = var.deployment
    MEMORY_BYTES      = var.server_memory_bytes
    PRIMARY_REGION    = var.peer_region == "" ? data.aws_region.current.name : var.peer_region
    QUORUM            = var.server_quorum
    REGION            = data.aws_region.current.name
    SETUP_DATA_VOLUME = file("${path.module}/files/setup-data-volume.sh")
    SHARDS            = var.server_shards
    ZOOKEEPER_ADDRESS = join(",", formatlist("%s:2181", aws_instance.zookeeper.*.private_ip))
  })

  vpc_security_group_ids = concat(
    [
      aws_security_group.server.id,
      aws_security_group.ssh.id
    ],
    var.extra_security_groups
  )

  tags = merge(
    {
      Name = format("%s-%s", local.server, count.index)
    },
    local.tags
  )
}

resource "aws_ebs_volume" "server" {
  count = var.server_instance_count

  availability_zone = aws_instance.server[count.index].availability_zone
  type              = "gp2"
  size              = var.server_volume_size
  encrypted         = var.encrypt_server_volume
  kms_key_id        = var.server_volume_kms_key_id

  tags = merge(
    {
      Name = format("%s-%s", local.server, count.index)
    },
    var.tags
  )
}

resource "aws_volume_attachment" "server" {
  count = var.server_instance_count

  device_name = local.device_name
  instance_id = aws_instance.server[count.index].id
  volume_id   = aws_ebs_volume.server[count.index].id
}
