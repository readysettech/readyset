#!/bin/sh
# When logging into an instance a message of the day will be displayed with
# OS information and the private hostname assigned by AWS via IMDS.

INSTANCE_ID="$(wget -qO- http://instance-data/latest/meta-data/instance-id)"
IMAGE_ID=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" | jq -r '.Reservations[0] .Instances[0] .ImageId')
PRIVATE_HOSTNAME=$(aws ec2 describe-images --image-ids "$IMAGE_ID" | jq -r '.Images[0] .Name' | sed 's:.*/::')

hostname "$PRIVATE_HOSTNAME"

cat > /etc/update-motd.d/00-header <<EOF
#!/bin/sh

[ -r /etc/lsb-release ] && . /etc/lsb-release

if [ -z "$DISTRIB_DESCRIPTION" ] && [ -x /usr/bin/lsb_release ]; then
        DISTRIB_DESCRIPTION=$(lsb_release -s -d)
fi

printf "Welcome to %s running %s (%s %s %s)\n" "$(hostname)" "$DISTRIB_DESCRIPTION" "$(uname -o)" "$(uname -r)"     "$(uname -m)"
EOF

chmod +x /etc/update-motd.d/*