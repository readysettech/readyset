### For the candidate:

variable "candidate_name" {
  type = string
}

variable "candidate_ip_address" {
  type = string
}

variable "candidate_ssh_public_key" {
  type = string
}

### For the interviewer

variable "interviewer_key_pair_name" {
  type = string
}

variable "extra_allowed_ips" {
  type    = list(string)
  default = []
}
