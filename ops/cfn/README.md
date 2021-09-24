# AWS Cloudformation templates

TODO: Implement all the templates described below.

`templates/readyset-mysql-super-template.yaml`: Creates a VPC, Bastion Host, RDS
database, Consul Server cluster, and Readyset cluster as with substacks.

`templates/readyset-authority-consul-template.yaml`: Creates a Consul cluster
with our AMIs that can be used by Readyset as an authority inside a given VPC.

`templates/readyset-mysql-template.yaml`: Creates a Readyset cluster of Adapters
and Servers in separate ASGs inside a given VPC.

# How to test using Taskcat locally

1. Create a regional bucket for Taskcat to upload in development stacks to. A
    reasonable name is of the format `readysettech-tcat-<username>-<region>`.
2. Create a file at `~/.taskcat.yml` with the following content:
```
general:
  s3_bucket: <Bucket Name created in step 1>
  regions:
    - <region>
  parameters:
    KeyPairName: <Name of AWS keypair>
    ReadySetBucketName: <Bucket Name created in step 1>
    AccessCIDR: <CIDR that you will be able to SSH to the Bastion Host from>
    DatabaseName: <Name of Database created in RDS>
```
3. Run `taskcat test run` from this directory. You can watch some progress from
    the console but otherwise you can now look at things in the AWS console as
    well.
4. Output will be put into `taskcat_outputs` and a `index.html` is viewable in
    a web browser.
