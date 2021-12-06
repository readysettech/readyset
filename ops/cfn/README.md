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


# Complete ('Super') Stack Setup
Development stacks are configured using CloudFormation templates in this directory.

## How-to
Items to replace are noted with angle brackets like this: `<Replace Me>`

We'll refer to your selected username as `<Username>` going forward.

Super-stacks are currently limited to the `us-east-2` region, referred to as `<Region>` going forward.

### Launching Your Own Stack
 1. Create an EC2 SSH Keypair, if you do not already have one
    a. Go to https://console.aws.amazon.com/ec2/v2/home#CreateKeyPair
    b. Create a key with your username as the name.
      * We'll refer to this as `<EC2 SSH Keypair name>`
    c. The default settings are OK
    d. **Important**: Download the generated `.pem` key file when prompted
 2. Run the stack creation shell script with the command `bash ..//cfn-test/create-mysql-super-stack.sh <EC2 SSH Keypair Name>`

# Testing CloudFormation Template Changes
[Taskcat](https://github.com/aws-quickstart/taskcat) is a tool for testing AWS CloudFormation templates.
We use `taskcat` to validate, launch, and update CloudFormation stacks in AWS.

## Taskcat Installation
Taskcat can be installed via pip.
```
pip3 install taskcat --user
```

## Taskcat Setup
 1. The EC2 SSH Keypair name should be the same as above
 2. Create an AWS S3 Bucket
    a. Go to https://s3.console.aws.amazon.com/s3/bucket/create
    b. Add a name for the bucket like: `readysettech-tcat-<Username>-<Region>`
      * We'll refer to this as `<S3 Bucket Name>`
    c. The default settings are OK
 3. Set the `CFN_BUCKET` environment variable in your shell to `<S3 Bucket Name>`
 4. Get your local IP by running: `curl ifconfig.me`
   * We'll refer to this as `<Access IP>`
 5. Create a local `taskcat` configuration file
    a. Create a file at `~/.taskcat.yml`
    b. The file contents should be as follows:
```
general:
  s3_bucket: <S3 Bucket Name>
  regions:
    - <Region>
  parameters:
    AccessCIDR: <Access IP>/32
    KeyPairName: <EC2 SSH Keypair name>
    ReadySetS3BucketName: <S3 Bucket Name>
    ReadySetDeploymentName: <EC2 SSH Keypair Name>
    DatabaseName: readyset
    # If deploying in us-east-2 sandbox (should be default)
    SSMParameterKmsKeyArn: arn:aws:kms:us-east-2:069491470376:key/5cb3afeb-e9dd-40df-98cf-a82bb53ee78b
    SSMPathRDSDatabasePassword: /readyset/sandbox/dbPassword
  ```

## Testing Template Changes
Taskcat can be used to check that a CloudFormation stack launches all of its designated resources. Note that this is just the infrastructure components, not any specific Readyset behavior.

`taskcat test run`: Starts a cloudformation stack. Outputs will be written to `taskcat outputs` and an `index.html` is viewable in a web browser.

The cloudformation stack will be torn down on both success and failure.

# Updating AMI Images From a Specific Build
 1. In Buildkite, go to the build with the AMIs you need.
 2. Ensure the following build jobs have run; click the button to run them
   a. `🚀 :rust: Build release binaries?`
   b. `packer: Build Readyset Packer images?`
 3. Look at the generated `packer-manifest.json` file
 4. Copy the AMI IDs from the manifest into `templates/readyset-mysql-template.yaml`
 5. Re-run the command to launch or test the stack, as needed
