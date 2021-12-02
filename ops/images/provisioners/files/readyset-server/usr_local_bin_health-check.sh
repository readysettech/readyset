#!/bin/bash
EC2_INSTANCE_ID=$(ec2metadata --instance-id)

filename=/tmp/.lasthealthcheck
declare -i timeout=30
declare -i last_check=$(( `date +%s` - `stat -L --format %Y $filename` ))

# last_check < timeout mins implies we might be in the middle of a check
# if we are killed in the middle of a check we will resume timeout mins later
if (($last_check > (30*60))) ; then

	nc -w $(($timeout*60)) -z 127.0.0.1 6033
	response=$?

	if [ "$response" != 0 ]; then
	    aws autoscaling set-instance-health --instance-id $EC2_INSTANCE_ID --health-status Unhealthy
	fi
	declare -i delay=$(($timeout+1))
	# Allow new healthcheck to run
	touch $filename -d "-$delay minute"
fi	
