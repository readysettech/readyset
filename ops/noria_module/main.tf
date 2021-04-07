data "aws_subnet_ids" "subnet_ids" {
  vpc_id = var.vpc_id
}
