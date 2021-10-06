# AWS Cloudformation templates

`templates/readyset-mysql-super-template.yaml`: Creates a VPC, Bastion Host, RDS
database, Consul Server cluster, and Readyset cluster as with substacks.

`templates/readyset-authority-consul-template.yaml`: Creates a Consul cluster
with our AMIs that can be used by Readyset as an authority inside a given VPC.

`templates/readyset-mysql-template.yaml`: Creates a Readyset cluster of Adapters
and Servers in separate ASGs inside a given VPC.

# Taskcat
[Taskcat](https://github.com/aws-quickstart/taskcat) is a tool for testing
AWS CloudFormation templates. We use it to test our Readyset CloudFormation
deployments.

Taskcat can be installed via pip.
```
pip3 install taskcat --user
```

# How to test using Taskcat

1. Create a regional bucket for Taskcat to upload in development stacks to. A
    reasonable name is of the format `readysettech-tcat-<username>-<region>`.
2. Create an EC2 keypair, or note the name of an existing EC2 keypair
3. Create a file at `~/.taskcat.yml` with the following content:
```
general:
  s3_bucket: <Bucket Name created in step 1>
  regions:
    - <region>
  parameters:
    KeyPairName: <Name of AWS keypair from step 2>
    ReadysetS3BucketName: <Bucket Name created in step 1>
    AccessCIDR: <CIDR that you will be able to SSH to the Bastion Host from>
```
3. Run `taskcat test run` from this directory. You can watch some progress from
    the console but otherwise you can now look at things in the AWS console as
    well.
4. Output will be put into `taskcat_outputs` and a `index.html` is viewable in
    a web browser.

# How to upload and use an in development CloudFormation stack using Taskcat

1. Ensure you have a `~/.taskcat.yaml` as defined in the how to test.
2. `taskcat upload` will upload the files in templates to the S3 bucket defined
    above.
3. Go into the AWS Console for CloudFormation for the Sandbox account
4. Click Create Stack
5. For Amazon S3 URL put:
    https://<bucket name>.s3.<region>.amazonaws.com/readyset/templates/readyset-mysql-super-template.yaml
6. Fill out the parameters screen.
   Make sure that ReadysetS3BucketName is configured to <bucket name> otherwise
   it will not pull the substack templates correctly.
7. Click Next until you get to the Review step. Make sure to check the two
    "I acknowledge" checkboxes and then click Create stack.

# How to update AMI images from a build

1. Make sure to unblock the release binaries step and the build packer images
    step.
2. Once build package images step has completed successfully, look at the
    artifact named packer-manifest.json that was generated.
3. Copy the AMI IDs from the manifest into
    `templates/readyset-mysql-template.yaml`
