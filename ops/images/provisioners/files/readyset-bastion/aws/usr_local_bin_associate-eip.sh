#!/usr/bin/env bash
set -e

IMDS_BASE_URL='http://169.254.169.254/latest'
TOKEN=""
EIP_LIST="${EIP_LIST}"

#------------- [ Functions ]---------------------------------#

function _query_assigned_public_ip() {
  # Note: ETH0 Only.
  # - Does not distinguish between EIP and Standard IP. Need to cross-ref later.
  echo "Querying the assigned public IP"
  PUBLIC_IP_ADDRESS=$(imds_request meta-data/public-ipv4/${ETH0_MAC}/public-ipv4s/)
}

function imdsv2_token() {
    curl -X PUT "${IMDS_BASE_URL}/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 600"
}

function imds_request() {
    REQUEST_PATH=$1
    if [[ -z $TOKEN ]]; then
        TOKEN=$(imdsv2_token)
    fi
    curl -sH "X-aws-ec2-metadata-token: $TOKEN" "${IMDS_BASE_URL}/${REQUEST_PATH}"
}

function setup_environment_variables() {
    REGION=$(imds_request meta-data/placement/availability-zone/)
    # ex: us-east-1a => us-east-1
    REGION=${REGION: :-1}
    ETH0_MAC=$(/sbin/ip link show dev eth0 | grep -E -o -i 'link/ether\ ([0-9a-z]{2}:){5}[0-9a-z]{2}' | /bin/sed -e 's,link/ether\ ,,g')
    INSTANCE_ID=$(imds_request meta-data/instance-id)
    _userdata_file="/var/lib/cloud/instance/user-data.txt"

    export REGION ETH0_MAC INSTANCE_ID
}

check_eip_state() {
  if [[ "${_eip_associated}" == "2" ]]; then
    echo "The Public IP address associated with eth0 (${PUBLIC_IP_ADDRESS}) is already an Elastic IP assigned to this box. Not proceeding further."
    exit 0;
  elif [[ "${_eip_associated}" == "1" ]]; then
    echo "EIP was associated, but to another instance. Let's keep searching for an available EIP."
  fi
}

function request_eip() {
  # Is the already-assigned Public IP an elastic IP?
  _query_assigned_public_ip

  set +e
  _determine_eip_assc_status ${PUBLIC_IP_ADDRESS}
  set -e

  check_eip_state


  echo "Evaluating list of potentially available EIPS: ${EIP_LIST}"
  # List of all EIPs coming in from User Data
  EIP_ARRAY=(${EIP_LIST//,/ })

  for eip in "${EIP_ARRAY[@]}"; do
    echo "Evaluating NIC association eligibility of EIP: ${eip}"
    if [[ "${eip}" == "Null" ]]; then
      echo "Detected a NULL Value, moving on."
      continue
    fi

    # Determine if the EIP has already been assigned.
    set +e
    _determine_eip_assc_status ${eip}
    set -e

    if [[ "${_eip_associated}" == "2" ]]; then
      echo "The Public IP address associated with eth0 (${PUBLIC_IP_ADDRESS}) is already an Elastic IP assigned to this box. Not proceeding further."
      exit 0;
    elif [[ "${_eip_associated}" == "1" ]]; then
      echo "EIP was associated, but to another instance. Let's keep searching for an available EIP."
      continue
    fi

    # Get allocation ID
    _get_eip_allocation_by_ip ${eip}
    if [[ "${eip_allocation}" == *"eipalloc"* && "${eip_assoc_id}" == *"eipassoc"* ]]; then
      echo "EIP is already allocated and associated: ${eip_allocation} ${eip_assoc_id}. Looking for another."
      continue
    fi

    # Attempt to assign EIP to the ENI.
    set +e
    aws ec2 associate-address \
      --instance-id ${INSTANCE_ID} \
      --allocation-id ${eip_allocation} \
      --region ${REGION} \
      --no-allow-reassociation
    rc=$?
    set -e

    if [[ ${rc} -ne 0 ]]; then
      echo "Unable to associate EIP ${eip}. This EIP was likely associated after we tried. Let's see if another EIP is available."
      continue
    else
      echo "Associated an EIP! This completes the search for an available EIP."
      break
    fi
  done

  echo "${FUNCNAME[0]} Ended"
}

function jitter_delay() {
  # To try and distribute times when EC2s are requesting EIPs.
  MINWAIT=1
  MAXWAIT=15
  sleep $((MINWAIT+RANDOM % (MAXWAIT-MINWAIT)))
}

function _determine_eip_assc_status(){
  echo "Determining EIP Association Status for [${1}]"
  set +e
  assoc_id=$(aws ec2 describe-addresses --public-ips ${1} --output json --region ${REGION} 2>/dev/null | jq -r .Addresses[0].AssociationId)
  instance_id=$(aws ec2 describe-addresses --public-ips ${1} --output json --region ${REGION} 2>/dev/null | jq -r .Addresses[0].InstanceId)
  set -e
  if [[ "${assoc_id}" == *"eipassoc"* ]]; then
    echo "EIP is already associated. Checking if it's for this host."
    if [[ "${INSTANCE_ID}" == "${instance_id}" ]]; then
      # Assigned to this instance
      _eip_associated=2
    else
      # Associated, but not to this instance, so find another
      _eip_associated=1
    fi
  else
    echo "EIP has not been associated already. Could be used."
    _eip_associated=0
  fi
}

function _get_eip_allocation_by_ip(){
  echo "Determining EIP Allocation for [${1}]"
  eip_allocation=$(aws ec2 describe-addresses --public-ips ${1} --output json --region "${REGION}" | jq -r .Addresses[0].AllocationId)
  eip_assoc_id=$(aws ec2 describe-addresses --public-ips ${1} --output json --region "${REGION}" | jq -r .Addresses[0].AssociationId)
}

#------------- [ Main ]---------------------------------#

setup_environment_variables

jitter_delay

request_eip
