# ensure-ebs-volume

## Summary
In Amazon Web Services (AWS), server instances generally only have ephemeral
block storage. For many systems that require persistent block storage, the
instances are set up on a permanent basis and can just be configured to
attach the same Elastic Block Storage (EBS) volume on start. However, for
ReadySet, we require persistent block storage in a clustering setup where
server instances can go away and on restart be able to find a volume to use.

ReadySet stores a copy of the data from the upstream database on the block
storage devices using [RocksDB](https://rocksdb.org/). The copies are associated
with a volume ID which can be used to find the data again even if they are
attached to different instances.

Some additional limitations to this from AWS is that EBS volumes may only be
attached to a single server instance at a time and if they are in the same
Availability Zone (AZ) as the instance. Each AWS region is split into 3 AZ and
AutoScaling Groups (ASG) which we launch ReadySet into will try their best to
keep the instances evenly balanced between the AZs.

This daemon also listens via SQS (Simple Queue Service) for notifications about
scaling events which have been configured to be sent by the ASG.

## Algorithm

First, fetch all of the EBS volumes tagged for this deployment. Then depending
on the results the instance takes one of a few different actions.

1. Filter the list of EBS volumes by the AZ and if the volume is available. If
    at least one volume is in such a state, attach that volume.
2. If total number of volumes is less than the total number of servers, create a
    new volume in this availabilty zone and attach it.
3. Otherwise, if no volumes are available in this AZ and the volumes have been
    created, fetch the list of instances for this deployment in the AZ.
4. If all the instances are up and healthy then this instance has been put into
    an AZ that has more instances than volumes. At this point, the instance sets
    itself to be unhealthy which will make AWS terminate it. This should allow
    AWS to try again in another AZ as it attempts to keep the number of
    instances per AZ constant.
5. Otherwise, at least one of the volumes is attached to an instance in an
    unhealthy/terminating state, which means the instance waits for the instance
    that is currently terminating to release its volume which the instance then
    takes for itself.

At the end of this process, the instance has either set itself to unhealthy or
has attached a volume.

The process continues to watch the SQS queue for the ASG for a Termination
message for the particular instance. This allows this process to unmount and
detach the volume cleanly and then inform AWS that this instance be terminated.
