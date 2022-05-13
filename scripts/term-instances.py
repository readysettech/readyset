'''
Script to terminate all instances not tagged with protected key
If ran as a lambda, will require refactoring with args/param handling
DryRun is enabled by default; will raise exception unless overriden
'''

import argparse
import boto3
import re
import sys

parser = argparse.ArgumentParser(description='Options for instance termination')
parser.add_argument('--dryrun', '-d', default=True, action='store_true',
                    help="enable or disable Dry Run of terminate method")
parser.add_argument('--region', '-r', required=True,
                    help="Please supply the AWS region to clean up")
parser.add_argument('--profile', '-p', required=False, default='default',
                    help="Please supply the AWS region to clean up")
args = parser.parse_args()
DRY_RUN = args.dryrun
REGION = args.region
PROFILE = args.profile
EXIT_MESSAGE = "You chose 'no'.  Exiting...\n"
REPROMPT_MESSAGE = "Please respond with 'yes' or 'no'\n"
WARNING_MESSAGE = "About to terminate: {}\nTerminate? yes/no\n"
TERM_MESSAGE = "Terminating Instances...\n"


# instances with tag key name matching PROTECTED_STRING are spared termination
PROTECTED_STRING = "owner"
TAG_KEY = re.compile(fr"\b{PROTECTED_STRING}\b", re.IGNORECASE)

# lefthand side of expr will eval to False if `tags` attr is NoneType
# will prevent eval of righthand side and NoneType iteration err
def instance_is_owned(instance):
    return instance.tags and any(TAG_KEY.match(t.get('Key')) for t in instance.tags)

# return list of instance IDs to terminate
def unowned_instances(instances):
    return [
        instance.id
        for instance in instances
        if not instance_is_owned(instance)
    ]

def get_confirmation():
    choice = input().lower()
    if choice == "yes":
        sys.stdout.write(TERM_MESSAGE)
        return True
    elif choice == "no":
        sys.stdout.write(EXIT_MESSAGE)
        sys.exit()
    else:
        sys.stdout.write(REPROMPT_MESSAGE)
        get_confirmation()

if __name__ == "__main__":
    # setup session and client
    session = boto3.session.Session(region_name=REGION, profile_name=PROFILE)
    ec2 = session.resource('ec2')

    # get collection of all instances
    all_instances = ec2.instances.all()

    # filter instances to be terminated
    to_terminate = unowned_instances(all_instances)

    # check to see if there are instances to terminate, prompt user
    if to_terminate:
        sys.stdout.write(WARNING_MESSAGE.format(to_terminate))
        if get_confirmation():
            result = ec2.instances.filter(InstanceIds=to_terminate).terminate(DryRun=DRY_RUN)
            print(result)
