use anyhow::Result;
use aws_sdk_ec2 as ec2;
use aws_sdk_rds as rds;
use ec2::model::Filter;
use rds::model::DbInstance;

pub(crate) mod subnets;

pub(crate) fn filter<K, V>(key: K, value: V) -> Filter
where
    K: Into<String>,
    V: Into<String>,
{
    Filter::builder().name(key).values(value).build()
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
