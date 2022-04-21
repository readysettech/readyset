# IRL

IRL is a social network that is looking to scale up using ReadySet.

## Status (2022-04-12)

IRL is looking to do split testing with ReadySet as a backend. To be more
specific, they intend to deploy a second version of ReadySet to a different set
of frontend machines. This second set of frontend machines will be configured to
use ReadySet as read replicas instead of their exist array of read replicas.
After that, they will send 1% of the traffic to the second deployment and see
how things go.

As of the last deployment, where there were also some internal issues around
queries leaking out regularly, IRL needed to update their code to support a
different set of credentials for read replicas.

## Technical Stack

IRL is running a PHP app with Laravel using the Eloquent ORM on top of MySQL.
They have maxed out the number of read replicas that one can create in AWS RDS.
They are deploying everything using CloudFormation.

## Access Information

The Deploy User account is configured using the template at
`ops/cfn/templates/readyset-deploy-user-template.yml`. This configures a user
that can log into the shared Readyset/IRL AWS account with mostly read only
permissions. The write permissions can be acquired by running CloudFormation
templates with the execution role also configured with it. The credentials
for this account are in [1Password][0]. Once logged in, you can create an
AWS access key/secret key pair in the IAM console to make a profile one can
access on the command line.

[0]: https://start.1password.com/open/i?a=ZK2P2YHFQ5CLDDI7T2ZUNF5QII&v=6nof7qxwnahbnrqfio4zo4ypkm&i=pxn2helf6t3ghq3ycrtpet5roq&h=team-readysettechnologyinc.1password.com

In addition, Harley Klein and TBD have Administrator credentials so they can
update the Deploy User template if needed or do other emergency updates.

## Deployment Instructions

Currently, we use a local CloudFormation template which uses the released
CloudFormation templates as substacks which we then release individual so
we can update them incrementally. The templates are located in `customers/irl`
with some of the parameters marked as needing to be updated with comments.

TODO: There is a missing file for the new monitoring component.

The suggestion is to copy those files to another location and run commands
similar to following for each one. These commands assume that the configured
profile for IRL is named `irl`.

```sh
aws cloudformation create-stack
  --profile irl \
  --region us-east-1 \
  --stack-name ue1-platform-readyset-<component> \
  --template-body file://<component>.yml \
  --role-arn <ARN of execution role from Deploy User template> \
  --disable-rollback \
  --capabilities CAPABILITY_IAM
```

This will need to be run for each component, waiting on the previous component
to be done and copying the needed values from the previous component to the new
component.
