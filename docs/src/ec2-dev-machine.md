# EC2 Developer Machine

## Prerequisites

TODO: Move these to part of the setup suggestions for general developers and
link to those instead.

1. Get your AWS sandbox account user credentials.
2. Generate an SSH key on your local machine.
3. Upload the SSH key to the AWS EC2 SSH keys.
4. Connect to the Tailscale VPN. `tailscale up` is the command for this.

## Create an instance

This [template][0] has been configured to set up an EC2 instance with Ubuntu
20.04 installed on it.

1. Click the link, authenticating with AWS if needed.
2. Set the instance type based upon what you need this instance for. For
  example, r5a.2xlarge is what we use in CI for builds. Please keep in mind
  pricing when you are looking at other sizes.
3. Set the key pair you have set up with AWS.
4. Add a tag named Owner and put your username as the value.
5. Click Launch Instance.

Once the instance launches, you will be able to SSH into the instance through
the private IP. Tailscale will handle the routing if you use the configuration
in this launch template.

You can copy your AWS credentials into this machine.

If you are using an especially large instance, suspend the instance during
extended time away.

[0]: https://us-east-2.console.aws.amazon.com/ec2/v2/home?region=us-east-2#LaunchInstanceFromTemplate:launchTemplateId=lt-0fc475ab63ec54bf0
