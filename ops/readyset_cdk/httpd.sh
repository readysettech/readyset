set -eu -o pipefail
export AWS_CLOUDFORMATION_STACK="${AWS::StackName}"
export AWS_CLOUDFORMATION_RESOURCE="ReadySetMySQLAdapterASG"
export AWS_CLOUDFORMATION_REGION="${AWS::Region}"
export CONSUL_TAG_KEY="${ConsulEc2RetryJoinTagKey}"
export CONSUL_TAG_VALUE="${ConsulEc2RetryJoinTagValue}"
export DEPLOYMENT="${ReadySetDeploymentName}"
export LOG_LEVEL="${ReadySetLogLevel}"
export EXTRA_ENV="${ReadySetAdapterExtraEnvironment}"
# Connection Parameters
export SSM_PATH_DB_PASSWORD=${SSMPathRDSDatabasePassword}
export USERNAME=${DatabaseAdapterUsername}
export DB_PROTO=${DatabaseProtocol}
export DB_HOST=${DatabaseHostname}
export DB_PORT=${DatabasePort}
export DB_NAME=${DatabaseName}
export ENABLE_CW=${EnableCloudwatch}
export ENABLE_AGG=${EnableAggregation}
exec /usr/local/bin/user-data-init.sh
