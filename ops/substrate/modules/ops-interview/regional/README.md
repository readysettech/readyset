# Ops interview module

## Example

```hcl
module "candidate_name_interview" {
  providers = {
    aws         = aws
    aws.network = aws
  }
  source = "../../../../../modules/ops-interview/regional"

  candidate_name           = "Candidate Name"
  candidate_ip_address     = "123.123.123.123"
  candidate_ssh_public_key = <<-EOT
ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDIwl+xQYRCk6Ijz/Ll8eXKZrcTH9/7xwlvIowiuqDSFtGkf+73QJkwVJ0YiKHWAPwIUWMzCEO/Ab2g6j4PcR+XYu8kXbrwT5aW65L/AK1oaav2RfV1bnQEVUP9FRPL52BN42J0ibI2QJZKJVws9JF7vxTWPPG0V0eoxcaRMk1ZEqq+/k3GuN8D69VSV8xo9lB8yZEvTxs0YQRiiF7Q6t/3jhYtz6lCdazQviRcSEOj5AVsDjcf1XIAPOcLK4Q4OEXL49T3UaitSYMyKIO8hzNLiyGAUlSbshAnutPXdyNBypkCs6FrSPSRdBfFjzUVE/a+JWCPmx0q0xAVd497Efxby+Vsa2/TPMp7tSisPaqk3MpPmjBS7eI/y4Pl2GpAB4OVANEBNd1Q6K2/37Pk+PrZtIUBiRG8sM0Od36BjwLCxvG0G5P/UYZ93aC8GzqkRf4evOBMiJCvR2o9CDEDycNyTm1y5dyJzQewOTWX9nsiF1rllc92W0ZALvpO03+W2+k= this-is-the-candidates-key
EOT

  interviewer_key_pair_name = "grfn"
  extra_allowed_ips         = ["67.80.61.199"] # you'll be able to ssh to the instance from this IP
}

output "instance_ip" {
  value = module.candidate_name_interview.instance_ip
}
```
