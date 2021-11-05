# AWS Cloudformation templates

`templates/readyset-mysql-super-template.yaml`: Creates a VPC, Bastion Host, RDS
database, Consul Server cluster, and Readyset cluster as with substacks.

`templates/readyset-authority-consul-template.yaml`: Creates a Consul cluster
with our AMIs that can be used by Readyset as an authority inside a given VPC.

`templates/readyset-mysql-template.yaml`: Creates a Readyset cluster of Adapters
and Servers in separate ASGs inside a given VPC.

`templates/dev.yaml.example`: Example template for a dev cloud formation stack.
This creates an instance of the `readyset-mysql-super-template.yaml` stack. It
should be copied to `templates/dev.yaml` and filled in where appropriate.

# Taskcat
[Taskcat](https://github.com/aws-quickstart/taskcat) is a tool for testing
AWS CloudFormation templates. We use it to test our Readyset CloudFormation
deployments and simplify updating templates for stacks.

Taskcat can be installed via pip.
```
pip3 install taskcat --user
```

# Getting started with a development stack.
We deploy our CloudFormation stacks from the templates hosted in S3 buckets.
To get started, a S3 bucket and an AWS EC2 keypair are needed:
  1. Create an AWS EC2 SSH keypair, or note the name of an existing AWS EC2 SSH
    keypair.
  2. Create a regional bucket for Taskcat to upload CloudFormation stacks to.
     A reasonable name is of the format `readysettech-tcat-<username>-<region>`.


We use the tool `Taskcat` to simplify the process of uploading changes to
stacks to S3 buckets. With the help of a little config, taskcat will take
`/templates` and upload updates to the s3 bucket.

  3. Create a file at `~/.taskcat.yml` with the following content:
  ```
  general:
    s3_bucket: <Bucket Name created in step 1>
    regions:
      - <region>
    parameters:
      AccessCIDR: <CIDR that you will be able to SSH to the Bastion Host from>
      KeyPairName: <Name of AWS EC2 SSH keypair from step 1>
      ReadySetS3BucketName: <Bucket Name created in step 2>
      ReadySetDeploymentName: <A valid ReadySet deployment name>
      DatabaseName: <Name of Database created in RDS>
  ```

Updates to the templates can be pushed to the S3 bucket with `taskcat upload`.

## Creating a stack through the CloudFormation console.
1. Go into the AWS Console for CloudFormation for the Sandbox account
2. Click Create Stack
3. For Amazon S3 URL put:
    https://<bucket name>.s3.<region>.amazonaws.com/readyset/templates/readyset-mysql-super-template.yaml
4. Fill out the parameters screen.
   Make sure that ReadySetS3BucketName is configured to <bucket name> otherwise
   it will not pull the substack templates correctly.
5. Click Next until you get to the Review step. Make sure to check the two
    "I acknowledge" checkboxes and then click Create stack.

## Creating a stack with the CLI.
1. `cp templates/dev.yaml.example templates/dev.yaml`.
2. Fill in required parameters in `templates/dev.yaml`.
3. `aws cloudformation create-stack --stack-name <stack name> --template-body file://<path to dev.yaml> --capabilities CAPABILITY_IAM`

# Testing with Taskcat
Taskcat can be used to check that a stack deployment works end-to-end. This only verifies that the AWS components start-up correctly, not any internal Readyset behavior.

`taskcat test run`: Starts a cloudformation stack. Outputs will be written to `taskcat outputs` and an `index.html` is viewable in a web browser.

Note: The cloudformation stack will be torn down on both success and failure.

# How to update AMI images from a build

1. Make sure to unblock the release binaries step and the build packer images
    step.
2. Once build package images step has completed successfully, look at the
    artifact named packer-manifest.json that was generated.
3. Copy the AMI IDs from the manifest into
    `templates/readyset-mysql-template.yaml`
