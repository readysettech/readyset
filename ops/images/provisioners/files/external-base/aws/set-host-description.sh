#!/bin/sh
# When logging into an instance a message of the day will be displayed with
# OS information and the private hostname assigned by AWS via IMDS.
INSTANCE_ID="$(wget -qO- http://169.254.169.254/latest/meta-data/instance-id)"
IMAGE_ID=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" | jq -r '.Reservations[0] .Instances[0] .ImageId')
IMAGE_NAME=$(aws ec2 describe-images --image-ids "$IMAGE_ID" | jq -r '.Images[0] .Name' | sed 's:.*/::')
SHORT_IMAGE_NAME=$(echo $IMAGE_NAME | cut -d"-" -f1,2,3)
PRIVATE_HOSTNAME="${SHORT_IMAGE_NAME}-${INSTANCE_ID}"

hostname "${PRIVATE_HOSTNAME}"

cat > /etc/update-motd.d/00-header <<EOF
#!/bin/sh

[ -r /etc/lsb-release ] && . /etc/lsb-release

if [ -z "$DISTRIB_DESCRIPTION" ] && [ -x /usr/bin/lsb_release ]; then
        DISTRIB_DESCRIPTION=$(lsb_release -s -d)
fi

printf "Welcome to %s running %s (%s %s %s)\n" "$(hostname)" "$DISTRIB_DESCRIPTION" "$(uname -o)" "$(uname -r)"     "$(uname -m)"
EOF

chmod +x /etc/update-motd.d/*
