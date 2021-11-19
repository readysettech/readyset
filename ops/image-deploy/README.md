# image-deploy

This defines [Packer](https://packer.io) builds to deploy Amazon Machine Images
(AMIs) from one account to another.

Amazon has no API for copying an image between accounts. The only way then to
move AMIs between account is to launch an instance with the AMI from the
source account in the target account and then call the API which creates an AMI
from an instance in the target account.
