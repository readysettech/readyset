#!/bin/bash
set -eux

# Set up a CFN stack, run basic sanity checks, and tear it down.
# These steps *must* be broken out into a script since buildkite does not play nicely with shell variable capture

cd ops/cfn
mkdir ./rendered_templates
buildkite-agent artifact download "*.yaml" --step packer-cfn-template-ytt ./rendered_templates
cd ./rendered_templates

# Generate an SSH key to connect to the bastion box
key_name="cfn-ci-key-${BUILDKITE_COMMIT}"

mkdir ~/keys
aws --region us-east-2 ec2 \
 create-key-pair \
 --key-name "$key_name" \
 --query 'KeyMaterial' \
 --output text > ~/keys/"$key_name".pem

chmod 400 ~/keys/"$key_name".pem

ssh_key_path="$(realpath ~/keys/${key_name}.pem)"

aws ssm put-parameter \
    --region us-east-2 \
    --name "/readyset/build/$key_name" \
    --type "SecureString" \
    --value file://"$ssh_key_path"

# We only capture the last line of script output since that's the only line that will contain the stack name.
mysql_super_stack=$(../../cfn-test/create-mysql-super-stack.sh -h "${BUILDKITE_COMMIT}" -k "$key_name" | tail -1)

aws cloudformation --region us-east-2 wait stack-create-complete --stack-name "$mysql_super_stack"

#Capture stack description in JSON format to parse later
stack_description=$(aws cloudformation --region us-east-2 describe-stacks --stack-name "$mysql_super_stack")

# Adapter variables
adapter_db_name=$(echo "$stack_description" | jq -r '.Stacks[0].Parameters | .[] | select(.ParameterKey == "DatabaseName").ParameterValue')
adapter_db_user=$(echo "$stack_description" | jq -r '.Stacks[0].Parameters | .[] | select(.ParameterKey == "DatabaseUsername").ParameterValue')
adapter_db_host=$(echo "$stack_description" | jq -r '.Stacks[0].Outputs | .[] | select(.OutputKey == "ReadySetAdapterNLBDNSName").OutputValue')
adapter_db_pass_path=$(echo "$stack_description" | jq -r '.Stacks[0].Parameters | .[] | select(.ParameterKey == "SSMPathRDSDatabasePassword").ParameterValue')
adapter_db_pass=$(aws ssm get-parameter --region us-east-2 --name "$adapter_db_pass_path" --with-decryption --query "Parameter.Value" --output text)
adapter_db_port=3306

# RDS variables
rds_host=$(echo "$stack_description" | jq -r '.Stacks[0].Outputs | .[] | select(.OutputKey == "ReadySetRDSHost").OutputValue')

# Monitor variables
prom_host=$(echo "$stack_description" | jq -r '.Stacks[0].Outputs | .[] | select(.OutputKey == "ReadySetMonitorLANIP").OutputValue')
prometheus_addr="${prom_host}:9091"

# MySQL connection strings
readyset_rds=mysql://$adapter_db_user:$adapter_db_pass@$rds_host:$adapter_db_port/$adapter_db_name
readyset_adapter=mysql://$adapter_db_user:$adapter_db_pass@$adapter_db_host:$adapter_db_port/$adapter_db_name

# Bastion instance information
bastion_eip=$(echo "$stack_description" | jq -r '.Stacks[0].Outputs | .[] | select(.OutputKey == "BastionHostEIP1").OutputValue')

ssh -o StrictHostKeyChecking=no \
  -o LogLevel=ERROR \
  -o UserKnownHostsFile=/dev/null \
  -i ~/keys/"$key_name".pem \
  ubuntu@"$bastion_eip" \
  basic_validation_test --help #check if we can reach the bastion box

ssh -o StrictHostKeyChecking=no \
  -o LogLevel=ERROR \
  -o UserKnownHostsFile=/dev/null \
  -i ~/keys/"$key_name".pem \
  ubuntu@"$bastion_eip" "basic_validation_test --adapter $readyset_adapter --rds $readyset_rds --prometheus-address $prometheus_addr --migration-mode explicit-migration"

#Clean up resources
aws --region us-east-2 cloudformation delete-stack --stack-name "$mysql_super_stack"
aws --region us-east-2 ec2 \
 delete-key-pair \
 --key-name "$key_name"
rm ~/keys/"$key_name".pem
aws ssm delete-parameter \
    --region us-east-2 \
    --name "/readyset/build/$key_name"