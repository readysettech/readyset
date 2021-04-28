resource "random_string" "random" {
  length  = 5
  special = false
}

locals {
  random = lower(random_string.random.id)
}
