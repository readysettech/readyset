use anyhow::{anyhow, bail, Result};
use cfn::model::Parameter as CfnParameter;
use console::style;
use ec2::model::{Filter, VpcAttributeName};
use futures::stream::{self, FuturesUnordered};
use futures::TryStreamExt;
use lazy_static::lazy_static;
use rds::model::{DbInstance, DbParameterGroup, Parameter};
use regex::Regex;
use {aws_sdk_cloudformation as cfn, aws_sdk_ec2 as ec2, aws_sdk_kms as kms, aws_sdk_rds as rds};

use crate::console::{spinner, GREEN_CHECK};

pub(crate) mod cloudformation;
pub(crate) mod subnets;

pub(crate) fn filter<K, V>(key: K, value: V) -> Filter
where
    K: Into<String>,
    V: Into<String>,
{
    Filter::builder().name(key).values(value).build()
}

pub(crate) fn cfn_parameter<K, V>(key: K, value: V) -> CfnParameter
where
    K: Into<String>,
    V: Into<String>,
{
    CfnParameter::builder()
        .parameter_key(key)
        .parameter_value(value)
        .build()
}

pub(crate) async fn rds_dbs_in_vpc(
    rds_client: &rds::Client,
    vpc_id: &str,
) -> Result<Vec<DbInstance>> {
    // no way in the API to filter RDS databases by VPC id, so we have to load them all then filter
    // by vpc after the fact :/
    let all_instances = rds_client
        .describe_db_instances()
        .send()
        .await?
        .db_instances
        .unwrap_or_default();

    Ok(all_instances
        .into_iter()
        .filter(|instance| {
            instance
                .db_subnet_group()
                .iter()
                .any(|sg| sg.vpc_id().iter().any(|vi| *vi == vpc_id))
        })
        .collect())
}

pub(crate) async fn db_parameters(
    rds_client: &rds::Client,
    db_instance: &DbInstance,
) -> Result<Vec<Parameter>> {
    db_instance
        .db_parameter_groups
        .iter()
        .flatten()
        .filter_map(|pg| pg.db_parameter_group_name())
        .map(|pgn| {
            rds_client
                .describe_db_parameters()
                .db_parameter_group_name(pgn)
                .send()
        })
        .collect::<FuturesUnordered<_>>()
        .map_ok(|res| stream::iter(res.parameters.unwrap_or_default().into_iter().map(Ok)))
        .try_flatten()
        .try_collect::<Vec<Parameter>>()
        .await
}

pub(crate) async fn vpc_cidr(ec2_client: &ec2::Client, vpc_id: &str) -> Result<String> {
    Ok(ec2_client
        .describe_vpcs()
        .vpc_ids(vpc_id)
        .send()
        .await?
        .vpcs
        .unwrap_or_default()
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("VPC went away!"))?
        .cidr_block
        .unwrap())
}

pub(crate) async fn wait_for_rds_db_available(rds_client: &rds::Client, db_id: &str) -> Result<()> {
    loop {
        let db = rds_client
            .describe_db_instances()
            .db_instance_identifier(db_id)
            .send()
            .await?
            .db_instances
            .unwrap_or_default()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("RDS database went away"))?;
        if db.db_instance_status() == Some("available") {
            break;
        }
    }

    Ok(())
}

pub(crate) async fn db_instance_parameter_group(
    rds_client: &rds::Client,
    db_instance: &DbInstance,
) -> Result<DbParameterGroup> {
    rds_client
        .describe_db_parameter_groups()
        .db_parameter_group_name(
            db_instance
                .db_parameter_groups
                .as_ref()
                .and_then(|pgs| pgs.first())
                .cloned()
                .ok_or_else(|| {
                    anyhow!(
                        "Could not find existing db parameter group for RDS database {}",
                        db_instance.db_instance_identifier().unwrap()
                    )
                })?
                .db_parameter_group_name()
                .unwrap(),
        )
        .send()
        .await?
        .db_parameter_groups
        .unwrap_or_default()
        .first()
        .cloned()
        .ok_or_else(|| {
            anyhow!(
                "Could not find existing db parameter group for RDS database {}",
                db_instance.db_instance_identifier().unwrap()
            )
        })
}

pub(crate) async fn reboot_rds_db_instance(rds_client: &rds::Client, db_id: &str) -> Result<()> {
    let reboot_db_desc = format!("RDS database instance {}", style(db_id).bold());
    let reboot_db_pb = spinner().with_message(format!("Rebooting {}", &reboot_db_desc));
    rds_client
        .reboot_db_instance()
        .db_instance_identifier(db_id)
        .send()
        .await?;
    wait_for_rds_db_available(rds_client, db_id).await?;
    reboot_db_pb.finish_with_message(format!("{}Rebooted {}", *GREEN_CHECK, reboot_db_desc));
    Ok(())
}

pub(crate) async fn vpc_attribute(
    ec2_client: &ec2::Client,
    vpc_id: &str,
    attribute: VpcAttributeName,
) -> Result<bool> {
    let attr = ec2_client
        .describe_vpc_attribute()
        .attribute(attribute.clone())
        .vpc_id(vpc_id)
        .send()
        .await?;
    let val = match attribute {
        VpcAttributeName::EnableDnsHostnames => attr.enable_dns_hostnames(),
        VpcAttributeName::EnableDnsSupport => attr.enable_dns_support(),
        _ => bail!("Unknown VPC attribute name"),
    };
    Ok(val.and_then(|val| val.value()).unwrap_or_default())
}

pub(crate) async fn kms_arn(kms_client: &kms::Client, key_id: impl Into<String>) -> Result<String> {
    let key_id = key_id.into();
    Ok(kms_client
        .describe_key()
        .key_id(&key_id)
        .send()
        .await?
        .key_metadata()
        .ok_or_else(|| anyhow!("Could not find KMS key {}", key_id))?
        .arn
        .clone()
        .unwrap())
}

/// Validate the given input string as an SSM parameter name according to the [rules for SSM
/// parameter names][docs]. This function is intended to be used as a [`dialoguer::Validator`]
///
/// [docs]: https://docs.aws.amazon.com/systems-manager/latest/APIReference/API_PutParameter.html#API_PutParameter_RequestSyntax
pub(crate) fn validate_ssm_parameter_name(input: &str) -> Result<(), &'static str> {
    lazy_static! {
        static ref INVALID_PREFIX_RE: Regex = Regex::new("(?i)^(?:aws|ssm)").unwrap();
        static ref VALID_CHARSET_RE: Regex = Regex::new("^[a-zA-Z0-9_./-]+$").unwrap();
    }

    if INVALID_PREFIX_RE.is_match(input) {
        return Err("Parameter names can't be prefixed with `aws` or `ssm`");
    }

    if !VALID_CHARSET_RE.is_match(input) {
        return Err("Invalid format for parameter name");
    }

    if input.split('/').count() > 15 {
        return Err("Parameter hierarchies are limited to a maximum depth of fifteen levels");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    mod validate_ssm_parameter_name {
        use super::*;

        fn is_valid(s: &str) {
            let res = validate_ssm_parameter_name(s);
            assert!(res.is_ok(), "Error: {}", res.err().unwrap());
        }

        fn is_invalid(s: &str) {
            assert!(validate_ssm_parameter_name(s).is_err())
        }

        #[test]
        fn valid_simple() {
            is_valid("MySecret")
        }

        #[test]
        fn valid_with_components() {
            is_valid("/Dev/Production/East/Project-ABC/MyParameter")
        }

        #[test]
        fn invalid_spaces() {
            is_invalid("My Secret")
        }

        #[test]
        fn invalid_charset() {
            is_invalid("Σecretσauce")
        }

        #[test]
        fn invalid_prefix() {
            is_invalid("aws/secrets")
        }

        #[test]
        fn invalid_prefix_with_case() {
            is_invalid("aWs/secrets")
        }

        #[test]
        fn invalid_too_many_segments() {
            is_invalid("a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p")
        }
    }
}
