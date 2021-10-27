# ensure-ebs-volume

## Summary
In Amazon Web Services (AWS), server instances generally only have ephemeral
block storage. For many systems that require persistent block storage, the
instances are set up on a permanent basis and can just be configured to
attach the same Elastic Block Storage (EBS) volume on start. However, for
ReadySet, we require persistent block storage in a clustering setup where
server instances can go away and on restart be able to find a volume to use.

Some additional limitations to this from AWS is that EBS volumes may only be
attached to a single server instance at a time and if they are in the same
Availability Zone (AZ) as the instance. Each AWS region is split into 3 AZ and
AutoScaling Groups (ASG) which we launch ReadySet into will try their best to
keep the instances evenly balanced between the AZs.

## Existing Algorithm

1. Find all the EBS volumes tagged for this inside the AZ for the instance this
    is launched from.
2. If the number of volumes availabe to be attached is 0, then create a new
    volume and attach it.
3. Otherwise, find one at random that is unattached and attach it.

Issue with this algorithm: During a instance refresh, previous instances are
terminated and are still holding onto the EBS volume as the new instance start.
The new instance finds all the volumes attached and so creates a new blank
volume.

## New Algorithm

1. Find all the EBS volumes currently tagged for this. If the number of volumes
    is less than the number of passed in instances, then create a new volume
    with the appropriate tags in the AZ this instance was launched in.
2. Filter the list of EBS volumes by the AZ which this instance is in. If one
    of them is unattached, attach that volume to the instance and finish
    successfully.
3. If all of the volumes in the AZ are attached and none of the instances those
    volumes are attached to are in an unhealthy/terminating state, then this
    instance started in a "full" AZ and needs to terminate itself.
4. If at least one of the volumes is attached to an instance in an
    unhealthy/terminating state, then report that this instance is healthy and
    then wait for the instance that is currently going away to release its
    volume to attach to this instance before continuing.
