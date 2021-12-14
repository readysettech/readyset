"""
await-mysql-super-stack.py

This script waits for a superstack to be created. After it has been created,
relevant stack outputs and EC2 instance information is outputted.

"""
import boto3
import sys
import argparse

# LogicalResourceIds for stack resources.
BASTION_STACK = "BastionStack"
READYSET_MYSQL_STACK = "ReadySetMySQLStack"
READYSET_RDS_STACK = "ReadySetRDSMySQLStack"
MONITOR_INSTANCE = "ReadySetMonitorInstance"


def get_stack_outputs(client, stack_id):
    outputs = {}
    stack = client.describe_stacks(StackName=stack_id)
    outputs_list = stack["Stacks"][0]["Outputs"]
    for o in outputs_list:
        outputs[o["OutputKey"]] = o["OutputValue"]
    return outputs


def bastion_outputs(client, stack_id):
    outputs = get_stack_outputs(client, stack_id)
    res = {}
    res["Bastion EIP"] = outputs["EIP1"]
    return res


def rds_outputs(client, stack_id):
    outputs = get_stack_outputs(client, stack_id)
    res = {}
    res["RDS Hostname"] = outputs["DatabaseHostname"]
    return res


def readyset_outputs(client, stack_id):
    outputs = get_stack_outputs(client, stack_id)
    res = {}
    res["Adapter NLB"] = outputs["ReadySetAdapterNLBDNSName"]

    monitor_resource = client.describe_stack_resource(
        StackName=stack_id,
        LogicalResourceId=MONITOR_INSTANCE,
    )["StackResourceDetail"]

    ec2_client = boto3.client("ec2")
    instance = ec2_client.describe_instances(
        InstanceIds=[monitor_resource["PhysicalResourceId"]]
    )["Reservations"][0]
    res["Monitor Instance IP"] = instance["Instances"][0]["PrivateIpAddress"]

    return res


def main():
    parser = argparse.ArgumentParser(
        description="Await the creation of a super stack.")
    parser.add_argument(
        "stack_name", type=str, help="an integer for the accumulator")
    args = parser.parse_args()

    stack_name = args.stack_name
    client = boto3.client("cloudformation")

    # Await stack creation using cloudformation wait.
    print("Waiting for stack to be created...")
    waiter = client.get_waiter("stack_create_complete")
    waiter.wait(StackName=stack_name)

    # Return the creation status.
    stack = client.describe_stacks(StackName=stack_name)["Stacks"][0]

    status = stack["StackStatus"]
    print("Stack creation completed with status: %s" % (status))

    if status != "CREATE_COMPLETE":
        print("Exiting...")

    # Get the identifiers for the relevant nested stacks so we can pull
    # information from each..
    stacks = {}
    response = client.describe_stack_resources(StackName=stack_name)
    for s in response["StackResources"]:
        stacks[s["LogicalResourceId"]] = s["PhysicalResourceId"]

    # Populate a dict with all the useful information.
    outputs = {}
    outputs.update(bastion_outputs(client, stacks[BASTION_STACK]))
    outputs.update(rds_outputs(client, stacks[READYSET_RDS_STACK]))
    outputs.update(readyset_outputs(client, stacks[READYSET_MYSQL_STACK]))

    print("")
    print("Stack Outputs")
    print("--------------------------------------")
    for key in sorted(outputs.keys()):
        print("%s: %s" % (key, outputs[key]))


if __name__ == "__main__":
    main()
