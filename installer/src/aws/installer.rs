use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use std::{env, iter};

use ::console::{style, Emoji};
use anyhow::{anyhow, bail, Result};
use aws_types::credentials::future::ProvideCredentials as ProvideCredentialsFut;
use aws_types::credentials::{CredentialsError, ProvideCredentials};
use aws_types::region::Region;
use aws_types::{Credentials, SdkConfig};
use ec2::model::{AttributeBooleanValue, InstanceType, KeyType, Subnet, VpcAttributeName};
use futures::stream::{FuturesUnordered, TryStreamExt};
use indicatif::MultiProgress;
use ipnet::Ipv4Net;
use itertools::Itertools;
use launchpad::display::EnglishList;
use lazy_static::lazy_static;
use rds::model::{ApplyMethod, DbInstance, DbParameterGroup, Parameter};
use regex::Regex;
use rusoto_credential::ProfileProvider;
use ssm::model::{ParameterStringFilter, ParameterType};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use {
    aws_sdk_cloudformation as cfn, aws_sdk_ec2 as ec2, aws_sdk_kms as kms, aws_sdk_rds as rds,
    aws_sdk_ssm as ssm,
};

use super::cloudformation::Template;
use crate::aws::cloudformation::{delete_stack, deploy_stack, stack_exists, StackConfig};
use crate::aws::{
    cfn_parameter, db_instance_parameter_group, filter, kms_arn, reboot_rds_db_instance,
    validate_ssm_parameter_name, vcpus_for_instance_type, vpc_attribute, vpc_cidr,
    wait_for_rds_db_available, STANDARD_VCPU_SERVICE_QUOTA_CODE,
};
use crate::console::{confirm, input, password, prompt_to_continue, select, spinner, GREEN_CHECK};
use crate::deployment::{
    CloudformationDeployment, CreateNew, DatabaseCredentials, DatabasePasswordParameter,
    Deployment, DeploymentData, DeploymentStatus, Engine, Existing, MaybeExisting, RdsDb,
    TemplateType,
};
use crate::{aws, Options};

/// List of regions where we deploy AMIs.
///
/// Should match `destination_regions` in `//ops/image-deploy/locals.pkr.hcl`
const REGIONS: &[&str] = &["us-east-1", "us-east-2", "us-west-2"];

/// Minimum number of availability zones in which we can deploy a cluster
const MIN_AVAILABILITY_ZONES: usize = 3;

/// Public cloudformation template for the VPC
const VPC_CLOUDFORMATION_TEMPLATE_URL: &str =
    "https://aws-quickstart.s3.amazonaws.com/quickstart-aws-vpc/templates/aws-vpc.template.yaml";

// Public cloudformation template prefix for the ReadySet stacks
// TODO: Not needed right now but would be good to have in the future.
// const READYSET_CLOUDFORMATION_S3_PREFIX: &str = readyset_cloudformation_s3_prefix!();

#[derive(Debug)]
struct RusotoAWSWrapper<T>(T);

impl<T> ProvideCredentials for RusotoAWSWrapper<T>
where
    T: rusoto_credential::ProvideAwsCredentials + Send + Sync + Debug,
{
    fn provide_credentials<'a>(&'a self) -> ProvideCredentialsFut<'a>
    where
        Self: 'a,
    {
        ProvideCredentialsFut::new(async {
            let creds = rusoto_credential::ProvideAwsCredentials::credentials(&self.0)
                .await
                .map_err(|e| CredentialsError::provider_error(Box::new(e)))?;
            Ok(Credentials::new(
                creds.aws_access_key_id(),
                creds.aws_secret_access_key(),
                creds.token().clone(),
                creds.expires_at().as_ref().map(|t| (*t).into()),
                "profile",
            ))
        })
    }
}

pub struct AwsInstaller<'a> {
    options: &'a mut Options,
    deployment: &'a mut Deployment,

    // Runtime state
    aws_config: Option<SdkConfig>,
    ec2_client: Option<ec2::Client>,
    rds_client: Option<rds::Client>,
    cfn_client: Option<cfn::Client>,
    ssm_client: Option<ssm::Client>,
    kms_client: Option<kms::Client>,
}

impl<'a> AwsInstaller<'a> {
    pub fn new(options: &'a mut Options, deployment: &'a mut Deployment) -> Self {
        Self {
            options,
            deployment,
            aws_config: None,
            ec2_client: None,
            rds_client: None,
            cfn_client: None,
            ssm_client: None,
            kms_client: None,
        }
    }

    /// Save this installer's deployment to the configured state directory
    pub async fn save(&self) -> Result<()> {
        self.deployment
            .save_to_directory(self.options.state_directory()?)
            .await
    }

    /// Returns a CFNDeployment if the inner deployment type matches, otherwise returns an error.
    /// This should only be used if you know for sure the deployment type is already a
    /// CFNDeployment.
    fn cfn_deployment(&mut self) -> Result<&mut CloudformationDeployment> {
        match self.deployment.inner {
            DeploymentData::Cloudformation(ref mut c) => Ok(c),
            _ => {
                // This should be unreachable in practice.
                bail!("Should not have run aws cloudformation deployment functionality unless our deployment type was for a CFN deployment.")
            }
        }
    }

    /// Run the install process for deploying to aws cloudformation, picking up where the user left
    /// off if necessary.
    pub async fn run(&mut self) -> Result<()> {
        // Unfortunately we have to do this at the top of each run method, because we can't mutably
        // borrow from self more than once (which would happen if we tried to pass in a mutable
        // borrow to CFNDeployment).
        self.save().await?;

        self.load_aws_config().await?;

        // Validate given AWS profile has access to Readyset images to avoid future errors
        self.validate_rs_ami_access().await?;

        self.save().await?;

        if let Some(vpc_id) = &self.cfn_deployment()?.vpc_id {
            match vpc_id {
                Existing(vpc_id) => println!("Using existing AWS VPC: {}", style(vpc_id).bold()),
                CreateNew => println!("Deploying to a {} AWS VPC", style("new").bold()),
            }
        } else {
            self.prompt_for_vpc().await?;
        };
        self.save().await?;

        if let Existing(vpc_id) = self.cfn_deployment()?.vpc_id.clone().unwrap() {
            self.validate_vpc(vpc_id).await?;
            self.save().await?;
        }

        if let Some(rds_db) = &self.cfn_deployment()?.rds_db {
            match &rds_db.db_id {
                CreateNew => println!(
                    "OK, we'll create a new {} RDS database instance.",
                    style(rds_db.engine).bold()
                ),
                Existing(rds_db_id) => println!(
                    "OK, we'll connect ReadySet with existing RDS database instance {}.",
                    style(rds_db_id).bold()
                ),
            }
        } else {
            self.prompt_for_rds_database().await?;
        }
        self.save().await?;

        let rds_db = self.cfn_deployment()?.rds_db.clone().unwrap();
        if let Existing(db_id) = rds_db.db_id {
            self.validate_rds_database(db_id, rds_db.engine).await?;
        }

        if self.cfn_deployment()?.database_credentials.is_some() {
            if matches!(
                select()
                    .with_prompt("Use existing database credentials, or enter new ones?")
                    .items(&["Existing credentials", "Enter new password"])
                    .interact()?,
                0
            ) {
                success!("Using previously-configured database credentials");
            } else {
                self.prompt_for_database_credentials().await?;
                self.save().await?;
            }
        } else {
            self.prompt_for_database_credentials().await?;
            self.save().await?;
        }

        if let Some(key_pair_name) = &self.cfn_deployment()?.key_pair_name {
            success!("Using SSH key pair: {}", key_pair_name)
        } else {
            self.configure_key_pair().await?;
        }
        self.save().await?;

        if self
            .cfn_deployment()?
            .vpc_id
            .as_ref()
            .unwrap()
            .is_create_new()
        {
            self.deploy_vpc().await?;
            self.save().await?;
        }

        if self
            .cfn_deployment()?
            .vpc_supplemental_stack_outputs
            .is_none()
        {
            self.deploy_vpc_supplemental_stack().await?;
            self.save().await?;
        }

        if self.cfn_deployment()?.consul_stack_outputs.is_none() {
            self.deploy_consul_stack().await?;
            self.save().await?;
        }

        if self
            .cfn_deployment()?
            .rds_db
            .as_ref()
            .unwrap()
            .db_id
            .is_create_new()
        {
            self.deploy_rds_db().await?;
            self.save().await?;
        }

        self.connect_db().await?;

        if self.cfn_deployment()?.readyset_stack_outputs.is_none() {
            self.deploy_readyset_cluster().await?;
        }

        self.deployment.status = DeploymentStatus::Complete;
        self.save().await?;

        println!(
            "\n{}",
            style(format!("{}Finished deploying ReadySet!", Emoji("✨ ", ""))).bold()
        );
        self.deployment.print_connection_information()?;

        Ok(())
    }

    async fn connect_db(&mut self) -> Result<()> {
        let security_group = self
            .cfn_deployment()?
            .vpc_supplemental_stack_outputs
            .as_ref()
            .unwrap()
            .get("ReadySetDBSecurityGroupID")
            .ok_or_else(|| {
                anyhow!("Could not find ReadySetDBSecurityGroupID output in VPC supplemental stack")
            })?
            .clone();
        let db_id = self
            .cfn_deployment()?
            .rds_db
            .as_ref()
            .unwrap()
            .db_id
            .as_existing()
            .unwrap()
            .clone();
        println!(
            "Adding security group {} to RDS database instance {}",
            style(&security_group).bold(),
            style(&db_id).bold()
        );
        prompt_to_continue()?;

        let mut security_groups = self
            .rds_client()
            .await?
            .describe_db_instances()
            .db_instance_identifier(&db_id)
            .send()
            .await?
            .db_instances
            .into_iter()
            .flatten()
            .next()
            .ok_or_else(|| anyhow!("RDS database instance {} went away", db_id))?
            .vpc_security_groups
            .unwrap_or_default()
            .into_iter()
            .filter_map(|sg| sg.vpc_security_group_id)
            .collect::<Vec<_>>();

        security_groups.push(security_group);

        let modify_pb = spinner().with_message("Modifying RDS database instance");
        self.rds_client()
            .await?
            .modify_db_instance()
            .db_instance_identifier(&db_id)
            .set_vpc_security_group_ids(Some(security_groups))
            .apply_immediately(true)
            .send()
            .await?;
        wait_for_rds_db_available(self.rds_client().await?, &db_id).await?;
        modify_pb.finish_with_message(format!("{}Modified RDS database instance", *GREEN_CHECK));

        Ok(())
    }

    async fn deploy_readyset_cluster(&mut self) -> Result<()> {
        let stack_name = self.deployment.readyset_stack_name();

        let template_url = match self.cfn_deployment()?.rds_db.as_ref().unwrap().engine {
            Engine::MySQL => self
                .cfn_deployment()?
                .cloudformation_template_url(TemplateType::Mysql),
            Engine::PostgreSQL => self
                .cfn_deployment()?
                .cloudformation_template_url(TemplateType::Postgres),
        };

        let deployment_name = self.deployment.name.clone();
        let key_pair_name = self.cfn_deployment()?.key_pair_name.clone().unwrap();
        let vpc_id = self
            .cfn_deployment()?
            .vpc_id
            .as_ref()
            .unwrap()
            .as_existing()
            .unwrap()
            .to_owned();
        let mut subnets = self
            .cfn_deployment()?
            .subnet_ids
            .clone()
            .unwrap()
            .into_iter();
        let consul_stack_outputs = self
            .cfn_deployment()?
            .consul_stack_outputs
            .as_ref()
            .unwrap();
        let retry_join_tag_key = consul_stack_outputs["ConsulEc2RetryJoinTagKey"].clone();
        let retry_join_tag_value = consul_stack_outputs["ConsulEc2RetryJoinTagValue"].clone();
        let consul_join_managed_policy_arn =
            consul_stack_outputs["ConsulJoinManagedPolicyArn"].clone();
        let supplemental_stack_outputs = self
            .cfn_deployment()?
            .vpc_supplemental_stack_outputs
            .clone()
            .unwrap();
        let DatabaseCredentials { username, password } =
            self.cfn_deployment()?.database_credentials.clone().unwrap();
        let database_hostname = self
            .rds_client()
            .await?
            .describe_db_instances()
            .db_instance_identifier(
                self.cfn_deployment()?
                    .rds_db
                    .as_ref()
                    .unwrap()
                    .db_id
                    .as_existing()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap()
            .db_instances
            .unwrap_or_default()
            .first()
            .ok_or_else(|| anyhow!("RDS database instance not found"))?
            .endpoint
            .as_ref()
            .unwrap()
            .address
            .as_ref()
            .unwrap()
            .clone();

        let mut stack_config = StackConfig::from_url(&template_url).await?;
        stack_config
            .with_non_modifiable_parameter("VPCID", vpc_id)
            .with_non_modifiable_parameter("PrivateSubnet1ID", subnets.next().unwrap())
            .with_non_modifiable_parameter("PrivateSubnet2ID", subnets.next().unwrap())
            .with_non_modifiable_parameter("PrivateSubnet3ID", subnets.next().unwrap())
            .with_non_modifiable_parameter("ConsulEc2RetryJoinTagKey", retry_join_tag_key)
            .with_non_modifiable_parameter("ConsulEc2RetryJoinTagValue", retry_join_tag_value)
            .with_non_modifiable_parameter(
                "ConsulJoinManagedPolicyArn",
                consul_join_managed_policy_arn,
            )
            .with_non_modifiable_parameter("KeyPairName", key_pair_name)
            .with_non_modifiable_parameter("ReadySetDeploymentName", deployment_name)
            .with_non_modifiable_parameter(
                "ReadySetServerSecurityGroupID",
                &supplemental_stack_outputs["ReadySetServerSecurityGroupID"],
            )
            .with_non_modifiable_parameter(
                "ReadySetAdapterSecurityGroupID",
                &supplemental_stack_outputs["ReadySetAdapterSecurityGroupID"],
            )
            .with_non_modifiable_parameter(
                "ReadySetMonitoringSecurityGroupID",
                &supplemental_stack_outputs["ReadySetMonitoringSecurityGroupID"],
            )
            .with_non_modifiable_parameter(
                "DatabaseName",
                self.cfn_deployment()?
                    .rds_db
                    .as_ref()
                    .unwrap()
                    .db_name
                    .clone(),
            )
            .with_non_modifiable_parameter("DatabaseHostname", database_hostname)
            .with_non_modifiable_parameter("DatabaseAdapterUsername", username)
            .with_non_modifiable_parameter("SSMParameterKmsKeyArn", password.kms_arn)
            .with_non_modifiable_parameter("SSMPathRDSDatabasePassword", password.ssm_path);

        stack_config.prompt_for_required_parameters()?;

        loop {
            println!("Deploying ReadySet cluster with the following config:");
            stack_config.describe_non_modifiable_config();
            match select()
                .with_prompt("Continue?")
                .items(&["yes", "no", "modify config"])
                .default(0)
                .interact()?
            {
                0 => break,
                1 => bail!("Exiting as requested"),
                2 => stack_config.modify_config()?,
                _ => unreachable!(),
            }
        }

        self.confirm_quotas(&stack_config).await?;

        let cfn_client = self.cfn_client().await?;
        let stack = deploy_stack(
            cfn_client,
            &stack_name,
            stack_config.apply_to_create_stack(
                cfn_client
                    .create_stack()
                    .stack_name(&stack_name)
                    .template_url(template_url)
                    .capabilities(cfn::model::Capability::CapabilityIam),
            ),
        )
        .await?;

        let outputs = stack
            .outputs
            .unwrap_or_default()
            .into_iter()
            .filter_map(|output| Some((output.output_key?, output.output_value?)))
            .collect();
        self.cfn_deployment()?.readyset_stack_outputs = Some(outputs);

        Ok(())
    }

    async fn confirm_quotas(&mut self, stack_config: &StackConfig) -> Result<()> {
        let parameter_value = |param| {
            stack_config
                .parameter_value(param)
                .ok_or_else(|| anyhow!("Parameter `{}` missing", param))
        };
        let adapter_instance_type =
            parameter_value("ReadySetAdapterInstanceType")?.parse::<InstanceType>()?;
        let server_instance_type =
            parameter_value("ReadySetServerInstanceType")?.parse::<InstanceType>()?;
        let monitor_instance_type =
            parameter_value("ReadySetMonitorInstanceType")?.parse::<InstanceType>()?;
        let adapter_nodes = parameter_value("ReadySetAdapterNodes")?.parse::<i32>()?;
        let server_nodes = parameter_value("ReadySetServerNodes")?.parse::<i32>()?;

        let ec2_client = self.ec2_client().await?;

        let total_needed_vcpus =
            (vcpus_for_instance_type(ec2_client, adapter_instance_type).await? * adapter_nodes)
                + (vcpus_for_instance_type(ec2_client, server_instance_type).await? * server_nodes)
                + vcpus_for_instance_type(ec2_client, monitor_instance_type).await?;

        let service_quotas_console_url = format!(
            "https://{}.console.aws.amazon.com/servicequotas/home/services/ec2/quotas/{}",
            self.aws_config()
                .await?
                .region()
                .ok_or_else(|| anyhow!("No region configured"))?,
            STANDARD_VCPU_SERVICE_QUOTA_CODE
        );

        println!(
            "The configuration you have selected for your ReadySet cluster requires at least {} \
             standard vCPUs. Ensure you have sufficient service quotas by logging into the AWS \
             console at {} before continuing.",
            total_needed_vcpus,
            style(service_quotas_console_url).bold()
        );

        if !confirm().with_prompt("Continue?").interact()? {
            bail!("Exiting as requested");
        }

        Ok(())
    }

    async fn deploy_rds_db(&mut self) -> Result<()> {
        println!("{}", style("About to deploy RDS database stack.").bold());
        prompt_to_continue()?;
        let stack_name = self.deployment.rds_stack_name();

        let template_url = match self.cfn_deployment()?.rds_db.as_ref().unwrap().engine {
            Engine::MySQL => self
                .cfn_deployment()?
                .cloudformation_template_url(TemplateType::RdsMysql),
            Engine::PostgreSQL => self
                .cfn_deployment()?
                .cloudformation_template_url(TemplateType::RdsPostgres),
        };

        let vpc_id = self
            .cfn_deployment()?
            .vpc_id
            .as_ref()
            .unwrap()
            .as_existing()
            .unwrap()
            .to_owned();
        let vpc_cidr = vpc_cidr(self.ec2_client().await?, &vpc_id).await?;
        let DatabaseCredentials { username, password } =
            self.cfn_deployment()?.database_credentials.clone().unwrap();
        let db_name = self
            .cfn_deployment()?
            .rds_db
            .as_ref()
            .unwrap()
            .db_name
            .clone();
        let mut subnets = self
            .cfn_deployment()?
            .subnet_ids
            .clone()
            .unwrap()
            .into_iter();

        let cfn_client = self.cfn_client().await?;
        let stack = deploy_stack(
            cfn_client,
            &stack_name,
            cfn_client
                .create_stack()
                .stack_name(&stack_name)
                .template_url(template_url)
                .parameters(cfn_parameter("VPCID", vpc_id))
                .parameters(cfn_parameter("VPCCIDR", vpc_cidr))
                .parameters(cfn_parameter("PrivateSubnet1ID", subnets.next().unwrap()))
                .parameters(cfn_parameter("PrivateSubnet2ID", subnets.next().unwrap()))
                .parameters(cfn_parameter("PrivateSubnet3ID", subnets.next().unwrap()))
                .parameters(cfn_parameter("DatabaseUsername", username))
                .parameters(cfn_parameter(
                    "SSMPathRDSDatabasePassword",
                    password.ssm_path,
                ))
                .parameters(cfn_parameter("DatabaseName", db_name)),
        )
        .await?;

        let rds_db_id = match stack
            .outputs
            .unwrap_or_default()
            .into_iter()
            .find(|output| output.output_key() == Some("DatabaseIdentifier"))
            .and_then(|output| output.output_value)
        {
            Some(id) => id,
            // TODO: remove once https://gerrit.readyset.name/c/readyset/+/1394 is deployed
            None => self
                .cfn_client()
                .await?
                .describe_stack_resource()
                .stack_name(&stack_name)
                .logical_resource_id("DatabaseInstance")
                .send()
                .await?
                .stack_resource_detail
                .ok_or_else(|| anyhow!("DatabaseInstance not found in RDS stack"))?
                .physical_resource_id
                .unwrap(),
        };
        self.cfn_deployment()?.rds_db.as_mut().unwrap().db_id = Existing(rds_db_id);

        Ok(())
    }

    async fn deploy_consul_stack(&mut self) -> Result<()> {
        println!("About to deploy Consul stack");
        prompt_to_continue()?;
        let stack_name = self.deployment.consul_stack_name();
        let template_url: String = self
            .cfn_deployment()?
            .cloudformation_template_url(TemplateType::Consul);

        let key_pair_name = self.cfn_deployment()?.key_pair_name.clone().unwrap();
        let consul_server_security_group_id = self
            .cfn_deployment()?
            .vpc_supplemental_stack_outputs
            .as_ref()
            .unwrap()["ConsulServerSecurityGroupID"]
            .clone();
        let mut subnets = self
            .cfn_deployment()?
            .subnet_ids
            .clone()
            .unwrap()
            .into_iter();

        let cfn_client = self.cfn_client().await?;
        let stack = deploy_stack(
            cfn_client,
            &stack_name,
            cfn_client
                .create_stack()
                .stack_name(&stack_name)
                .template_url(template_url)
                .parameters(cfn_parameter("KeyPairName", &key_pair_name))
                .parameters(cfn_parameter("PrivateSubnet1ID", subnets.next().unwrap()))
                .parameters(cfn_parameter("PrivateSubnet2ID", subnets.next().unwrap()))
                .parameters(cfn_parameter("PrivateSubnet3ID", subnets.next().unwrap()))
                .parameters(cfn_parameter(
                    "ConsulEc2RetryJoinTagKey",
                    "ReadySetConsulNodeType",
                ))
                .parameters(cfn_parameter("ConsulEc2RetryJoinTagValue", "Server"))
                .parameters(cfn_parameter(
                    "ConsulServerSecurityGroupID",
                    consul_server_security_group_id,
                ))
                .capabilities(cfn::model::Capability::CapabilityIam),
        )
        .await?;

        let outputs = stack
            .outputs
            .unwrap_or_default()
            .into_iter()
            .filter_map(|output| Some((output.output_key?, output.output_value?)))
            .collect();
        self.cfn_deployment()?.consul_stack_outputs = Some(outputs);

        Ok(())
    }

    async fn deploy_vpc_supplemental_stack(&mut self) -> Result<()> {
        println!(
            "{}",
            style("About to deploy VPC supplemental stack.").bold()
        );
        println!(
            "The VPC supplemental stack contains the fundamental networking components to support \
             a performant and secure implementation of ReadySet.\nHere are some of the components \
             deployed in this stack:
             - VPC endpoints for services like: CloudFormation (for signaling), SSM, CloudWatch, \
               CloudWatch Logs, KMS, SQS, Autoscaling, and EC2
             - VPC security groups for Consul server, ReadySet Server, ReadySet Adapter, \
               Monitoring (Prometheus), and one to be applied to your RDS DB instance"
        );
        prompt_to_continue()?;
        let stack_name = self.deployment.vpc_supplemental_stack_name();
        let template_url = self
            .cfn_deployment()?
            .cloudformation_template_url(TemplateType::VpcSupplemental);

        let vpc_id = self
            .cfn_deployment()?
            .vpc_id
            .as_ref()
            .unwrap()
            .as_existing()
            .unwrap()
            .to_owned();
        let vpc_cidr = vpc_cidr(self.ec2_client().await?, &vpc_id).await?;

        let default_security_group_id = self
            .ec2_client()
            .await?
            .describe_security_groups()
            .filters(filter("vpc-id", &vpc_id))
            .filters(filter("group-name", "default"))
            .send()
            .await?
            .security_groups
            .unwrap_or_default()
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("Could not find default VPC security group"))?
            .group_id
            .unwrap();

        let subnet_ids = self
            .cfn_deployment()?
            .subnet_ids
            .as_ref()
            .unwrap()
            .join(",");

        let cfn_client = self.cfn_client().await?;
        let stack = deploy_stack(
            cfn_client,
            &stack_name,
            cfn_client
                .create_stack()
                .stack_name(&stack_name)
                .template_url(template_url)
                .parameters(cfn_parameter("VPCPrivateSubnetIds", subnet_ids))
                .parameters(cfn_parameter("VPCID", vpc_id))
                .parameters(cfn_parameter("VPCCIDR", vpc_cidr))
                .parameters(cfn_parameter("AdditionalAdapterCIDR", "0.0.0.0/16"))
                .parameters(cfn_parameter(
                    "BastionSecurityGroupID",
                    default_security_group_id,
                )),
        )
        .await?;

        let outputs = stack
            .outputs
            .unwrap_or_default()
            .into_iter()
            .filter_map(|output| Some((output.output_key?, output.output_value?)))
            .collect();
        self.cfn_deployment()?.vpc_supplemental_stack_outputs = Some(outputs);

        Ok(())
    }

    async fn deploy_vpc(&mut self) -> Result<()> {
        println!(
            "{}",
            style("About to create new VPC to deploy ReadySet to.").bold()
        );
        prompt_to_continue()?;

        let stack_name = self.deployment.vpc_stack_name();

        let mut azs = self
            .ec2_client()
            .await?
            .describe_availability_zones()
            .filters(filter(
                "region-name",
                self.cfn_deployment()?.aws_region.as_ref().unwrap(),
            ))
            .send()
            .await?
            .availability_zones
            .unwrap_or_default()
            .into_iter()
            .filter_map(|az| az.zone_name);

        let cfn_client = self.cfn_client().await?;
        let stack = deploy_stack(
            cfn_client,
            &stack_name,
            cfn_client
                .create_stack()
                .stack_name(&stack_name)
                .template_url(VPC_CLOUDFORMATION_TEMPLATE_URL)
                .parameters(cfn_parameter("AvailabilityZones", azs.join(",")))
                .parameters(cfn_parameter("NumberOfAZs", "3")),
        )
        .await?;

        let stack_outputs = stack.outputs.unwrap_or_default();
        let vpc_id = stack_outputs
            .iter()
            .find(|output| output.output_key() == Some("VPCID"))
            .and_then(|output| output.output_value())
            .ok_or_else(|| anyhow!("CloudFormation stack missing VPCID output key"))?;

        self.cfn_deployment()?.vpc_id = Some(Existing(vpc_id.to_owned()));

        let subnet_ids = stack_outputs
            .iter()
            .filter(|output| {
                [
                    "PrivateSubnet1AID",
                    "PrivateSubnet2AID",
                    "PrivateSubnet3AID",
                ]
                .contains(&output.output_key().unwrap())
            })
            .filter_map(|output| output.output_value.clone())
            .collect();

        self.cfn_deployment()?.subnet_ids = Some(subnet_ids);

        Ok(())
    }

    async fn prompt_for_database_credentials(&mut self) -> Result<()> {
        let rds_db = self.cfn_deployment()?.rds_db.as_ref().unwrap();
        if rds_db.db_id.is_existing() {
            println!("ReadySet needs credentials to connect to your RDS database.");
            println!("This user needs to have at least the following permissions:");
            let permissions = match rds_db.engine {
                Engine::MySQL => &[
                    "SELECT",
                    "LOCK TABLES",
                    "SHOW DATABASES",
                    "REPLICATION SLAVE",
                    "REPLICATION CLIENT",
                ][..],
                Engine::PostgreSQL => &["REPLICATION", "SELECT", "CREATE"][..],
            };
            for permission in permissions {
                println!(" • {}", style(permission).blue())
            }
        } else {
            println!("Next, enter credentials for the root user account of the new RDS database.");
        }
        let username = input().with_prompt("Database username").interact_text()?;

        println!("ReadySet supports loading your database instance password from AWS Systems Manager Parameter Store");
        println!("You can provide either an existing SecureString parameter, or enter the password directly and we can create one for you");
        let password_kind = select()
            .with_prompt("Use an existing SSM parameter, or enter one directly?")
            .items(&["Existing SSM Parameter", "Enter password directly"])
            .interact()?;

        let password = if password_kind == 0 {
            // existing ssm parameter
            let ssm_parameters = self
                .ssm_client()
                .await?
                .describe_parameters()
                .parameter_filters(
                    ParameterStringFilter::builder()
                        .key("Type")
                        .option("Equals")
                        .values("SecureString")
                        .build(),
                )
                .send()
                .await?
                .parameters
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            let selection = select()
                .with_prompt("SSM Parameter to use")
                .items(
                    &ssm_parameters
                        .iter()
                        .map(|param| param.name.clone().unwrap())
                        .collect::<Vec<_>>(),
                )
                .interact()?;
            let parameter = &ssm_parameters[selection];

            let kms_arn = kms_arn(
                self.kms_client().await?,
                parameter
                    .key_id
                    .clone()
                    .unwrap_or_else(|| "alias/aws/ssm".into()),
            )
            .await?;

            DatabasePasswordParameter {
                ssm_path: parameter.name.clone().unwrap(),
                kms_arn,
            }
        } else {
            // enter password directly
            let password = password().with_prompt("Database Password").interact()?;
            println!("We'll save that password to a new AWS SSM parameter.");
            let parameter_name = input()
                .with_prompt("SSM Parameter path to create")
                .validate_with(|input: &String| validate_ssm_parameter_name(input))
                .interact()?;
            let create_pb = spinner().with_message(format!(
                "Creating SSM Parameter {}",
                style(&parameter_name).bold()
            ));
            self.ssm_client()
                .await?
                .put_parameter()
                .name(&parameter_name)
                .value(password)
                .r#type(ParameterType::SecureString)
                .send()
                .await?;
            create_pb.finish_with_message(format!(
                "{}Created SSM Parameter {}",
                *GREEN_CHECK,
                style(&parameter_name).bold()
            ));

            DatabasePasswordParameter {
                ssm_path: parameter_name,
                kms_arn: kms_arn(self.kms_client().await?, "alias/aws/ssm").await?,
            }
        };

        self.cfn_deployment()?.database_credentials =
            Some(DatabaseCredentials { username, password });

        Ok(())
    }

    async fn configure_key_pair(&mut self) -> Result<()> {
        let answer = select()
            .with_prompt(
                "Use an existing SSH key pair for the instances, or create a new one?\n\
                 If existing, it must be registered in your AWS account.",
            )
            .items(&["Existing key pair", "New key pair"])
            .default(1)
            .interact()?;
        let key_pair_name = if answer == 0 {
            let mut key_pairs = self
                .ec2_client()
                .await?
                .describe_key_pairs()
                .send()
                .await?
                .key_pairs
                .unwrap_or_default()
                .into_iter()
                .filter_map(|kp| kp.key_name)
                .collect::<Vec<_>>();
            let idx = select()
                .items(&key_pairs)
                .with_prompt("Key pair to use")
                .interact()?;
            key_pairs.remove(idx)
        } else {
            println!("Creating and installing a new SSH key pair to allow you to log in to the ReadySet cluster");
            let key_pair_name: String = input()
                .with_prompt("SSH key pair name")
                .default("readyset".to_owned())
                .interact_text()?;
            let key_pair_pb = spinner().with_message(format!(
                "Creating SSH key pair {}",
                style(&key_pair_name).bold()
            ));
            let key_pair = self
                .ec2_client()
                .await?
                .create_key_pair()
                .key_name(&key_pair_name)
                .key_type(KeyType::Ed25519)
                .send()
                .await?;
            key_pair_pb.finish_with_message(format!(
                "{}Created SSH key pair {}",
                *GREEN_CHECK,
                style(&key_pair_name).bold(),
            ));

            let default_key_pair_path = PathBuf::from(env::var("HOME").unwrap())
                .join(".ssh")
                .join(format!("{}.pem", key_pair_name))
                .to_string_lossy()
                .into_owned();
            let key_pair_path = PathBuf::from(
                input()
                    .with_prompt("Path to save key pair to:")
                    .default(default_key_pair_path)
                    .interact()?,
            );
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .append(true)
                .open(&key_pair_path)
                .await?;
            file.write_all(key_pair.key_material().unwrap().as_bytes())
                .await?;

            key_pair_name
        };

        self.cfn_deployment()?.key_pair_name = Some(key_pair_name);

        success!("Configured SSH key");

        Ok(())
    }

    async fn validate_rs_ami_access(&mut self) -> Result<()> {
        let futures = FuturesUnordered::new();
        for template_url in [
            self.cfn_deployment()?
                .cloudformation_template_url(TemplateType::Mysql),
            self.cfn_deployment()?
                .cloudformation_template_url(TemplateType::Postgres),
        ] {
            let template = Template::download(template_url).await?;
            let region = self.cfn_deployment()?.aws_region.as_ref().unwrap();

            if let Some(mappings) = template.mappings {
                if let Some(region_map) = mappings.aws_ami_region_map.get(region) {
                    for ami_id in region_map.values() {
                        let ec2 = self.ec2_client().await?.clone();
                        let ami_id = ami_id.to_owned();
                        futures.push(async move {
                            let result = ec2
                                .describe_images()
                                .filters(filter("image-id", ami_id))
                                .send()
                                .await;

                            result.map(|image_output| {
                                !image_output.images.unwrap_or_default().is_empty()
                            })
                        });
                    }
                }
            }
        }

        let res: Vec<bool> = futures.try_collect().await?;
        let has_ami_access = res.iter().all(|b| *b);
        if !has_ami_access {
            bail!(
                "This AWS account doesn't have access to the AMIs needed for installation. Please use a different AWS account for this deployment."
            )
        }

        Ok(())
    }

    /// Retrieves public subnets for the given VPC id. If none exist, HashSet returned will be
    /// empty.
    async fn retrieve_public_subnets(&mut self, vpc_id: &str) -> Result<HashSet<String>> {
        let route_tables = self
            .ec2_client()
            .await?
            .describe_route_tables()
            .filters(filter("vpc-id", vpc_id))
            .send()
            .await?
            .route_tables
            .unwrap_or_default();
        Ok(route_tables
            .into_iter()
            .filter_map(|route_table| {
                let public = route_table
                    .routes()
                    .unwrap_or_default()
                    .iter()
                    // A public subnet is defined as a subnet that has a route table whereby any
                    // route directs to an internet gateway. An internet gateway id is always
                    // prefixed by igw.
                    .any(|r| r.gateway_id().unwrap_or_default().starts_with("igw"));
                if public {
                    let subnets: Vec<String> = route_table
                        .associations
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|a| a.subnet_id)
                        .collect();
                    Some(subnets)
                } else {
                    None
                }
            })
            .flatten()
            .collect())
    }

    async fn validate_vpc(&mut self, mut vpc_id: String) -> Result<()> {
        // First, check that the VPC actually exists
        let vpcs = self
            .ec2_client()
            .await?
            .describe_vpcs()
            .filters(filter("vpc-id", &vpc_id))
            .send()
            .await?;
        let vpc = if let Some(vpc) = vpcs.vpcs.unwrap_or_default().into_iter().next() {
            vpc
        } else {
            println!(
                "The previously configured VPC {} no longer exists, or you're missing the required \
                 permissions to view it",
                style(&vpc_id).bold()
            );
            if confirm()
                .with_prompt("Would you like to change the configured VPC?")
                .interact()?
            {
                self.prompt_for_vpc().await?;
                self.save().await?;

                match self.cfn_deployment()?.vpc_id.as_ref().unwrap() {
                    CreateNew => return Ok(()),
                    Existing(new_vpc_id) => {
                        vpc_id = new_vpc_id.clone();
                        self.ec2_client()
                            .await?
                            .describe_vpcs()
                            .filters(filter("vpc-id", &vpc_id))
                            .send()
                            .await?
                            .vpcs
                            .unwrap()
                            .into_iter()
                            .next()
                            .unwrap()
                    }
                }
            } else {
                bail!("Could not successfully access the configured VPC");
            }
        };

        let vpc_cidr: Ipv4Net = vpc
            .cidr_block()
            .ok_or_else(|| anyhow!("VPC with id {} doesn't have any CIDR blocks", vpc_id))?
            .parse()?;

        let dns_hostnames = vpc_attribute(
            self.ec2_client().await?,
            &vpc_id,
            VpcAttributeName::EnableDnsHostnames,
        )
        .await?;
        let dns_support = vpc_attribute(
            self.ec2_client().await?,
            &vpc_id,
            VpcAttributeName::EnableDnsSupport,
        )
        .await?;
        if !dns_hostnames || !dns_support {
            println!("ReadySet requires the EnableDnsHostnames and EnableDnsSupport VPC attributes to both be enabled for the VPC");
            println!("If you like, I can automatically fix your VPC's configuration");
            prompt_to_continue()?;

            let modify_pb = spinner().with_message("Modifying VPC attributes");
            if !dns_hostnames {
                self.ec2_client()
                    .await?
                    .modify_vpc_attribute()
                    .vpc_id(&vpc_id)
                    .enable_dns_hostnames(AttributeBooleanValue::builder().value(true).build())
                    .send()
                    .await?;
            }
            if !dns_support {
                self.ec2_client()
                    .await?
                    .modify_vpc_attribute()
                    .vpc_id(&vpc_id)
                    .enable_dns_support(AttributeBooleanValue::builder().value(true).build())
                    .send()
                    .await?;
            }
            modify_pb.finish_with_message(format!("{}Modified VPC attributes", *GREEN_CHECK));
        }

        let subnets = self
            .ec2_client()
            .await?
            .describe_subnets()
            .filters(filter("vpc-id", &vpc_id))
            .send()
            .await?
            .subnets
            .unwrap_or_default();

        let mut subnets_by_az: HashMap<String, Vec<&Subnet>> = HashMap::new();
        for subnet in &subnets {
            subnets_by_az
                .entry(
                    subnet
                        .availability_zone
                        .clone()
                        .ok_or_else(|| anyhow!("Subnet missing availability zone"))?,
                )
                .or_default()
                .push(subnet)
        }

        let public_subnets = self.retrieve_public_subnets(&vpc_id).await?;
        // Detects a common setup where each availability zone has exactly one private and one
        // public subnet.
        let uses_pub_private_design = |subnets_by_az: &HashMap<String, Vec<&Subnet>>| -> bool {
            for subnets in subnets_by_az.values() {
                if subnets.len() != 2 {
                    return false;
                }
                let first_is_pub =
                    public_subnets.contains(subnets[0].subnet_id().unwrap_or_default());
                let second_is_pub =
                    public_subnets.contains(subnets[1].subnet_id().unwrap_or_default());
                if first_is_pub == second_is_pub {
                    return false;
                }
            }
            true
        };
        let mut existing_subnet_ids: Vec<String> = vec![];
        if uses_pub_private_design(&subnets_by_az) {
            println!("Found one private and one public sunet in each availability zone.");
            let use_pub = matches!(
                select()
                    .with_prompt("Which subnet would you like to use?")
                    .items(&["Public", "Private"])
                    .interact()?,
                0
            );
            let mut out: Vec<String> = subnets_by_az
                .values()
                .map(|subnets| {
                    subnets
                        .iter()
                        .filter_map(|s| {
                            let s_id = s.subnet_id().unwrap_or_default();
                            let is_pub = public_subnets.contains(s_id);
                            if use_pub == is_pub {
                                Some(s_id.to_owned())
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .collect();
            existing_subnet_ids.append(&mut out);
        } else {
            // Iterate through AZs, getting one subnet per AZ.
            for (az, subnets) in &subnets_by_az {
                if subnets.is_empty() {
                    // No subnets in this az, let's try the next one.
                    continue;
                }
                let subnet_ids: Vec<&str> =
                    subnets.iter().map(|s| s.subnet_id().unwrap()).collect();
                if subnet_ids.len() > 1 {
                    println!("Found more than one subnet in availability zone {}", az);
                    let idx = select()
                        .with_prompt("Which subnet would you like to use?")
                        .items(&subnet_ids)
                        .interact()?;
                    existing_subnet_ids.push(subnet_ids[idx].to_string())
                } else {
                    existing_subnet_ids.push(subnet_ids.first().unwrap().to_string());
                }
            }
        }

        if existing_subnet_ids.len() < MIN_AVAILABILITY_ZONES {
            let other_azs = self
                .ec2_client()
                .await?
                .describe_availability_zones()
                .filters(filter(
                    "region-name",
                    self.cfn_deployment()?.aws_region.as_ref().unwrap(),
                ))
                .send()
                .await?
                .availability_zones
                .unwrap_or_default()
                .into_iter()
                .filter_map(|az| az.zone_name)
                .filter(|zone_name| !subnets_by_az.contains_key(zone_name.as_str()))
                .take(MIN_AVAILABILITY_ZONES - existing_subnet_ids.len());

            println!(
                "ReadySet needs to deploy into at least 3 subnets in different availability zones"
            );
            println!(
                "The selected VPC {} only has the following subnets:",
                style(&vpc_id).bold(),
            );
            for subnet in &subnets {
                println!(
                    " • {}, in {}",
                    style(subnet.cidr_block().unwrap()).blue(),
                    style(subnet.availability_zone().unwrap()).bold()
                );
            }

            let new_cidrs = aws::subnets::subnet_cidrs(
                vpc_cidr,
                subnets
                    .iter()
                    .map(|subnet| {
                        Ok(subnet
                            .cidr_block()
                            .ok_or_else(|| {
                                anyhow!(
                                    "Subnet {} is missing a CIDR block!",
                                    subnet.subnet_id().unwrap()
                                )
                            })?
                            .parse()?)
                    })
                    .collect::<Result<Vec<Ipv4Net>>>()?,
                MIN_AVAILABILITY_ZONES,
            )?;

            let subnet_az_cidrs = other_azs.zip(new_cidrs).collect::<Vec<_>>();

            println!("If you like, I can automatically create the following new subnets:",);
            for (az, cidr) in &subnet_az_cidrs {
                println!(" • {}, in {}", style(cidr).blue(), style(az).bold());
            }

            if confirm()
                .with_prompt("Create subnets?")
                .default(true)
                .interact()?
            {
                let mut subnet_ids = self.create_subnets(vpc_id, subnet_az_cidrs).await?;
                subnet_ids.extend(existing_subnet_ids);
                self.cfn_deployment()?.subnet_ids = Some(subnet_ids);
            } else {
                bail!(
                    "Please create subnets in the missing availability zones in your VPC, then \
                     re-run this installer to continue"
                );
            }
        } else {
            success!(
                "VPC has subnets in at least {} availability zones",
                MIN_AVAILABILITY_ZONES
            );
            self.cfn_deployment()?.subnet_ids = Some(existing_subnet_ids)
        }

        Ok(())
    }

    async fn create_subnets(
        &mut self,
        vpc_id: String,
        subnet_az_cidrs: Vec<(String, Ipv4Net)>,
    ) -> Result<Vec<String>> {
        let multi_bar = MultiProgress::new();
        let ec2_client = self.ec2_client().await?;
        let futures = FuturesUnordered::new();
        for (az, cidr) in subnet_az_cidrs {
            let bar = multi_bar
                .add(spinner().with_message(format!("Creating subnet {} in {}", cidr, az)));
            bar.enable_steady_tick(50);
            let ec2 = ec2_client.clone();
            let vpc_id = vpc_id.to_owned();
            futures.push(async move {
                let res = ec2
                    .create_subnet()
                    .vpc_id(vpc_id)
                    .availability_zone(az)
                    .cidr_block(cidr.to_string())
                    .send()
                    .await;
                match &res {
                    Ok(subnet) => bar.finish_with_message(format!(
                        "{}Created {}",
                        *GREEN_CHECK,
                        style(subnet.subnet().unwrap().subnet_id().unwrap()).bold()
                    )),
                    Err(e) => bar.finish_with_message(format!("{}Failed: {}", *GREEN_CHECK, e)),
                }
                res.map(|subnet| subnet.subnet.unwrap().subnet_id.unwrap())
            })
        }

        let res = futures.try_collect().await;
        multi_bar.join()?;
        Ok(res?)
    }

    async fn validate_rds_database(&mut self, mut db_id: String, mut engine: Engine) -> Result<()> {
        let instance = match self
            .rds_client()
            .await?
            .describe_db_instances()
            .db_instance_identifier(&db_id)
            .send()
            .await?
            .db_instances
            .unwrap_or_default()
            .first()
        {
            Some(instance) => instance.clone(),
            None => {
                println!(
                    "The previously configured RDS db instance {} no longer exists, or you're \
                          missing the required permissions to view it",
                    style(&db_id).bold()
                );
                if confirm()
                    .with_prompt("Would you like to change the configured RDS database instance?")
                    .interact()?
                {
                    self.prompt_for_rds_database().await?;
                    self.save().await?;

                    let rds_db = self.cfn_deployment()?.rds_db.as_ref().unwrap();
                    match &rds_db.db_id {
                        CreateNew => return Ok(()),
                        Existing(new_db_id) => {
                            db_id = new_db_id.clone();
                            engine = rds_db.engine;
                            self.rds_client()
                                .await?
                                .describe_db_instances()
                                .db_instance_identifier(&db_id)
                                .send()
                                .await?
                                .db_instances
                                .unwrap_or_default()
                                .remove(0)
                        }
                    }
                } else {
                    bail!("Could not successfully access the configured RDS database instance");
                }
            }
        };

        match engine {
            Engine::MySQL => self.validate_rds_database_mysql(db_id, instance).await,
            Engine::PostgreSQL => self.validate_rds_database_postgresql(db_id, instance).await,
        }
    }

    async fn validate_rds_database_mysql(
        &mut self,
        db_id: String,
        db_instance: DbInstance,
    ) -> Result<()> {
        let mut wrong_binlog_format = false;
        let parameters = aws::db_parameters(self.rds_client().await?, &db_instance).await?;
        if parameters
            .iter()
            .filter(|p| p.parameter_name() == Some("binlog_format"))
            .any(|p| p.parameter_value() != Some("ROW"))
        {
            warning!(
                "ReadySet requires the MySQL {} parameter to be set to {}",
                style("binlog_format").blue(),
                style("ROW").blue()
            );
            wrong_binlog_format = true;
        }

        let mut wrong_backup_retention_period = false;
        if db_instance.backup_retention_period() == 0 {
            warning!(
                "ReadySet requires the RDS {} to be greater than 0",
                style("backup_retention_period").blue()
            );
            wrong_backup_retention_period = true;
        }

        if !(wrong_binlog_format || wrong_backup_retention_period) {
            success!("RDS database {} has the correct configuration", db_id);
            return Ok(());
        }

        println!(
            "I can automatically fix the configuration of the RDS database instance {} to be \
             compatible with ReadySet",
            style(&db_id).bold()
        );
        println!(
            "{}{}{}",
            Emoji("🚨  ", ""),
            style("WARNING: This will reboot your database instance!")
                .bold()
                .red(),
            Emoji(" 🚨", ""),
        );

        if !confirm()
            .with_prompt("Automatically fix database configuration?")
            .interact()?
        {
            bail!("Please ensure your database configuration is correct, then re-run the installer")
        }

        let parameter_group_name = if wrong_binlog_format {
            let existing_parameter_group =
                db_instance_parameter_group(self.rds_client().await?, &db_instance).await?;
            Some(
                self.create_parameter_group(
                    "readyset-mysql",
                    existing_parameter_group,
                    parameters
                        .into_iter()
                        .filter(|param| param.parameter_name() != Some("binlog_format"))
                        .chain(iter::once(
                            Parameter::builder()
                                .parameter_name("binlog_format")
                                .parameter_value("ROW")
                                .apply_method(ApplyMethod::Immediate)
                                .build(),
                        ))
                        .collect::<Vec<_>>(),
                )
                .await?,
            )
        } else {
            None
        };

        let mut updates = vec![];
        if let Some(parameter_group_name) = &parameter_group_name {
            updates.push(format!(
                "{}={}",
                style("db_parameter_group_name").blue(),
                style(&parameter_group_name).blue(),
            ))
        }
        if wrong_backup_retention_period {
            updates.push(format!(
                "{}={}",
                style("backup_retention_period").blue(),
                style(1).blue()
            ))
        }

        let modify_db_desc = format!(
            "RDS database instance {}: {}",
            style(&db_id).bold(),
            updates.into_and_list()
        );
        let modify_db_pb = spinner().with_message(format!("Modifying {}", &modify_db_desc));
        modify_db_pb.enable_steady_tick(50);

        let mut req = self
            .rds_client()
            .await?
            .modify_db_instance()
            .db_instance_identifier(&db_id);
        if let Some(parameter_group_name) = &parameter_group_name {
            req = req.db_parameter_group_name(parameter_group_name);
        }
        if wrong_backup_retention_period {
            req = req.backup_retention_period(1);
        }
        req.send().await?;
        modify_db_pb.finish_with_message(format!("{}Modified {}", *GREEN_CHECK, modify_db_desc));

        reboot_rds_db_instance(self.rds_client().await?, &db_id).await?;

        Ok(())
    }

    async fn validate_rds_database_postgresql(
        &mut self,
        db_id: String,
        db_instance: DbInstance,
    ) -> Result<()> {
        let parameters = aws::db_parameters(self.rds_client().await?, &db_instance).await?;
        if parameters
            .iter()
            .filter(|p| p.parameter_name() == Some("rds.logical_replication"))
            .any(|p| p.parameter_value() != Some("1"))
        {
            warning!(
                "ReadySet requires the {} parameter to be set to {}",
                style("rds.logical_replication").blue(),
                style("1").blue()
            );
        } else {
            success!("RDS database {} has the correct configuration", db_id);
            return Ok(());
        }

        let existing_parameter_group =
            db_instance_parameter_group(self.rds_client().await?, &db_instance).await?;
        let parameter_group_name = self
            .create_parameter_group(
                "readyset-postgresql",
                existing_parameter_group,
                parameters
                    .into_iter()
                    .filter(|param| param.parameter_name() != Some("rds.logical_replication"))
                    .chain(iter::once(
                        Parameter::builder()
                            .parameter_name("rds.logical_replication")
                            .parameter_value("ROW")
                            .apply_method(ApplyMethod::Immediate)
                            .build(),
                    ))
                    .collect::<Vec<_>>(),
            )
            .await?;

        let modify_db_desc = format!(
            "RDS database instance {}: setting {}={}",
            style(&db_id).bold(),
            style("rds_logical_replication").blue(),
            style("1").blue(),
        );
        let modify_db_pb = spinner().with_message(format!("Modifying {}", modify_db_desc));
        self.rds_client()
            .await?
            .modify_db_instance()
            .db_instance_identifier(&db_id)
            .db_parameter_group_name(&parameter_group_name)
            .send()
            .await?;
        modify_db_pb.finish_with_message(format!("{}Modified {}", *GREEN_CHECK, modify_db_desc));

        reboot_rds_db_instance(self.rds_client().await?, &db_id).await?;

        Ok(())
    }

    /// Create a new RDS parameter group with the given (default) name based on the given existing
    /// parameter group, except with the given set of parameters.
    ///
    /// Returns the `parameter_group_name` of the created db parameter group
    async fn create_parameter_group<N: Into<String>>(
        &mut self,
        default_name: N,
        existing_parameter_group: DbParameterGroup,
        parameters: Vec<Parameter>,
    ) -> Result<String> {
        // first, check if a parameter group with the default_name already exists
        let default_name: String = default_name.into();
        let existing = self
            .rds_client()
            .await?
            .describe_db_parameter_groups()
            .db_parameter_group_name(&default_name)
            .send()
            .await?
            .db_parameter_groups
            .unwrap_or_default();
        let (parameter_group_name, do_create) = if existing.is_empty() {
            (Cow::Borrowed(&default_name), true)
        } else {
            println!(
                "Found an existing RDS DB parameter group named {}",
                style(&default_name).bold()
            );
            if confirm()
                .with_prompt(
                    "Would you like to update that parameter group to set the correct parameters?",
                )
                .interact()?
            {
                (Cow::Borrowed(&default_name), false)
            } else {
                let parameter_group_name = input()
                    .with_prompt("Please enter a name for a DB parameter group to create")
                    .interact()?;
                (Cow::Owned(parameter_group_name), true)
            }
        };

        // Figure out the default parameters that group gets created with
        let default_parameters = self
            .rds_client()
            .await?
            .describe_db_parameters()
            .db_parameter_group_name(parameter_group_name.as_ref())
            .send()
            .await?
            .parameters
            .unwrap_or_default()
            .into_iter()
            .map(|param| (param.parameter_name, param.parameter_value))
            .collect::<HashSet<_>>();

        let parameters = parameters
            .into_iter()
            .filter(|param| {
                !default_parameters
                    .contains(&(param.parameter_name.clone(), param.parameter_value.clone()))
            })
            .collect::<Vec<_>>();

        let parameters_desc = parameters
            .iter()
            .filter_map(|param| {
                Some(format!(
                    "{} = {}",
                    style(param.parameter_name()?).blue(),
                    style(param.parameter_value()?).blue()
                ))
            })
            .collect::<Vec<_>>()
            .into_and_list();

        let (verbing, verbed) = if do_create {
            ("Creating new", "Created new")
        } else {
            ("Updating", "Updated")
        };
        let pb_desc = format!(
            "RDS parameter group {} based on {} with {}",
            style("readyset").bold(),
            style(existing_parameter_group.db_parameter_group_name().unwrap()).bold(),
            parameters_desc,
        );
        let parameter_group_pb = spinner().with_message(format!("{} {}", verbing, pb_desc));
        parameter_group_pb.enable_steady_tick(50);

        if do_create {
            self.rds_client()
                .await?
                .create_db_parameter_group()
                .db_parameter_group_name(parameter_group_name.as_ref())
                .db_parameter_group_family(
                    existing_parameter_group
                        .db_parameter_group_family()
                        .unwrap(),
                )
                .description("Automatically-created DB parameter group for ReadySet")
                .send()
                .await?
                .db_parameter_group
                .unwrap();
        }

        self.rds_client()
            .await?
            .modify_db_parameter_group()
            .db_parameter_group_name(parameter_group_name.as_ref())
            .set_parameters(Some(parameters))
            .send()
            .await?;

        parameter_group_pb.finish_with_message(format!("{}{} {}", *GREEN_CHECK, verbed, &pb_desc,));

        Ok(parameter_group_name.into_owned())
    }

    async fn prompt_for_rds_database(&mut self) -> Result<()> {
        let deploy_new_instance = |vpc_id: &str| -> Result<bool> {
            if confirm()
                .with_prompt(
                    "Would you like to deploy a new RDS database instance to the current VPC?",
                )
                .default(true)
                .interact()?
            {
                println!(
                    "OK, we'll create a new RDS database instance in {}.",
                    vpc_id
                );
                Ok(true)
            } else {
                Ok(false)
            }
        };
        let rds_db = if let Existing(vpc_id) = self.cfn_deployment()?.vpc_id.clone().unwrap() {
            println!(
                "ReadySet will keep cached query results up-to-date based on data changes in \
                 your database.\n"
            );
            if confirm()
                .with_prompt(
                    "Would you like to connect ReadySet to an existing RDS database instance?",
                )
                .default(false)
                .interact()?
            {
                let mut instances = aws::rds_dbs_in_vpc(self.rds_client().await?, &vpc_id).await?;
                if instances.is_empty() {
                    println!("No RDS database instances found in {}", vpc_id);
                    if deploy_new_instance(&vpc_id)? {
                        None
                    } else {
                        bail!("No RDS database instance found in {} and you have elected to not deploy a new one. We can't proceed with deployment without a RDS database instance.", vpc_id);
                    }
                } else {
                    println!(
                        "Found {} RDS database instances in {}",
                        style(instances.len()).bold(),
                        style(&vpc_id).bold()
                    );
                    let idx = select()
                        .with_prompt("Which RDS database instance would you like to connect to?")
                        .items(
                            &instances
                                .iter()
                                .map(|db_instance| db_instance.db_instance_identifier().unwrap())
                                .collect::<Vec<_>>(),
                        )
                        .interact()?;

                    let instance = instances.remove(idx);
                    let engine = Engine::from_aws_engine(
                        instance
                            .engine()
                            .ok_or_else(|| anyhow!("RDS instance missing engine"))?,
                    )?;
                    let mut input = input();
                    input.with_prompt(format!("Which {} database should we connect to?", engine));
                    if let Some(instance_db) = instance.db_name() {
                        input.default(instance_db.to_owned());
                    }
                    let db_name = input.interact_text()?;
                    Some(RdsDb {
                        db_id: Existing(
                            instance
                                .db_instance_identifier
                                .ok_or_else(|| anyhow!("RDS instance missing identifier"))?,
                        ),
                        db_name,
                        engine,
                    })
                }
            } else if deploy_new_instance(&vpc_id)? {
                None
            } else {
                bail!("You have elected to not use an existing RDS instance in vpc {}, and have also elected to not deploy a new RDS instance. We can't proceed without an RDS database instance.", &vpc_id);
            }
        } else {
            None
        }
        .map::<Result<_>, _>(Ok)
        .unwrap_or_else(|| {
            let engine = self.deployment.db_type;

            let db_name = input()
                .with_prompt("Enter a name for the database:")
                .validate_with(|input: &String| {
                    lazy_static! {
                        static ref RE: Regex = Regex::new("^[a-zA-Z][a-zA-Z0-9]{0,63}$").unwrap();
                    }
                    if RE.is_match(input) {
                        Ok(())
                    } else {
                        Err(
                            "Database name must be less than 63 characters, begin with a letter, \
                             and contain only alphanumeric characters",
                        )
                    }
                })
                .interact()?;

            Ok(RdsDb {
                db_id: CreateNew,
                db_name,
                engine,
            })
        })?;

        self.cfn_deployment()?.rds_db = Some(rds_db);

        Ok(())
    }

    async fn prompt_for_vpc(&mut self) -> Result<MaybeExisting<&str>> {
        println!(
            "If you plan to use ReadySet with an existing database, we should deploy to the \
             same VPC as that database. Otherwise, we can create a new VPC."
        );
        let vpc_id = if confirm()
            .with_prompt("Would you like to deploy to an existing AWS VPC?")
            .default(false)
            .interact()?
        {
            let vpcs = self.ec2_client().await?.describe_vpcs().send().await?;
            let vpcs = vpcs
                .vpcs()
                .into_iter()
                .flatten()
                .flat_map(|vpc| {
                    let vpc_id = vpc.vpc_id.as_ref()?;
                    let vpc_name = vpc
                        .tags()
                        .into_iter()
                        .flatten()
                        .find_map(|tag| {
                            if tag.key() == Some("Name") {
                                tag.value()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(vpc_id);
                    Some((vpc_id, vpc_name))
                })
                .collect::<Vec<_>>();

            let vpc_names = vpcs.iter().map(|(_, name)| name).collect::<Vec<_>>();

            println!(
                "Found {} VPCs in {}",
                style(vpcs.len()).bold(),
                style(self.cfn_deployment()?.aws_region.as_ref().unwrap()).bold()
            );
            let idx = select()
                .with_prompt("Which VPC should we deploy to?")
                .items(&vpc_names)
                .interact()?;

            Existing(vpcs[idx].0.to_owned())
        } else {
            println!("OK, we'll create a new AWS VPC as part of the deployment process.");
            CreateNew
        };

        Ok(self.cfn_deployment()?.vpc_id.insert(vpc_id).as_deref())
    }

    async fn ec2_client(&mut self) -> Result<&ec2::Client> {
        if self.ec2_client.is_none() {
            self.init_ec2_client().await?;
        }
        Ok(self.ec2_client.as_ref().unwrap())
    }

    async fn init_ec2_client(&mut self) -> Result<&ec2::Client> {
        let ec2_client = ec2::Client::new(self.aws_config().await?);
        Ok(self.ec2_client.insert(ec2_client))
    }

    async fn rds_client(&mut self) -> Result<&rds::Client> {
        if self.rds_client.is_none() {
            self.init_rds_client().await?;
        }
        Ok(self.rds_client.as_ref().unwrap())
    }

    async fn init_rds_client(&mut self) -> Result<&rds::Client> {
        let rds_client = rds::Client::new(self.aws_config().await?);
        Ok(self.rds_client.insert(rds_client))
    }

    async fn cfn_client(&mut self) -> Result<&cfn::Client> {
        if self.cfn_client.is_none() {
            self.init_cfn_client().await?;
        }
        Ok(self.cfn_client.as_ref().unwrap())
    }

    async fn init_cfn_client(&mut self) -> Result<&cfn::Client> {
        let cfn_client = cfn::Client::new(self.aws_config().await?);
        Ok(self.cfn_client.insert(cfn_client))
    }

    async fn ssm_client(&mut self) -> Result<&ssm::Client> {
        if self.ssm_client.is_none() {
            self.init_ssm_client().await?;
        }
        Ok(self.ssm_client.as_ref().unwrap())
    }

    async fn init_ssm_client(&mut self) -> Result<&ssm::Client> {
        let ssm_client = ssm::Client::new(self.aws_config().await?);
        Ok(self.ssm_client.insert(ssm_client))
    }

    async fn kms_client(&mut self) -> Result<&kms::Client> {
        if self.kms_client.is_none() {
            self.init_kms_client().await?;
        }
        Ok(self.kms_client.as_ref().unwrap())
    }

    async fn init_kms_client(&mut self) -> Result<&kms::Client> {
        let kms_client = kms::Client::new(self.aws_config().await?);
        Ok(self.kms_client.insert(kms_client))
    }

    async fn aws_config(&mut self) -> Result<&SdkConfig> {
        if self.aws_config.is_none() {
            self.load_aws_config().await?;
        }
        Ok(self.aws_config.as_ref().unwrap())
    }

    async fn load_aws_config(&mut self) -> Result<&SdkConfig> {
        let mut loader = aws_config::from_env();

        let profile = if let Some(aws_credentials_profile) =
            &self.cfn_deployment()?.aws_credentials_profile
        {
            println!(
                "Using AWS profile: {}",
                style(aws_credentials_profile).bold()
            );
            aws_credentials_profile
        } else {
            self.prompt_for_aws_credentials_profile()?
        };

        loader = loader.credentials_provider(RusotoAWSWrapper(
            ProfileProvider::with_default_credentials(profile)?,
        ));

        let region = if let Some(aws_region) = &self.cfn_deployment()?.aws_region {
            println!("Using AWS region: {}", style(aws_region).bold());
            aws_region
        } else {
            self.prompt_for_aws_region()?
        };

        loader = loader.region(Region::new(region.to_owned()));

        let config = loader.load().await;

        success!("Loaded AWS credentials");

        Ok(self.aws_config.insert(config))
    }

    fn prompt_for_aws_credentials_profile(&mut self) -> Result<&str> {
        println!(
            "Please provide the AWS credentials profile (in {}) we should use",
            style("~/.aws/credentials").bold()
        );

        let profile = input()
            .with_prompt("Which AWS profile should we use?")
            .default("default".to_owned())
            .validate_with(|input: &String| {
                match ProfileProvider::with_default_credentials(input) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.message),
                }
            })
            .interact_text()?;
        println!();

        Ok(self
            .cfn_deployment()?
            .aws_credentials_profile
            .insert(profile))
    }

    fn prompt_for_aws_region(&mut self) -> Result<&str> {
        let mut prompt = select();
        println!(
            "Select an AWS region to deploy to. This should be the same region as your \
             primary database, if you plan to use ReadySet with an existing database."
        );
        prompt.with_prompt("Which AWS region would you like to deploy to?");
        prompt.items(REGIONS);

        if let Ok(default_region) = env::var("AWS_DEFAULT_REGION") {
            if let Some(idx) = REGIONS.iter().position(|r| r == &default_region) {
                prompt.default(idx);
            }
        }

        let idx = prompt.interact()?;

        Ok(self
            .cfn_deployment()?
            .aws_region
            .insert(REGIONS[idx].to_owned()))
    }

    pub async fn tear_down(&mut self) -> Result<()> {
        for stack in [
            self.deployment.readyset_stack_name(),
            self.deployment.rds_stack_name(),
            self.deployment.consul_stack_name(),
            self.deployment.vpc_supplemental_stack_name(),
            self.deployment.vpc_stack_name(),
        ] {
            if stack_exists(self.cfn_client().await?, &stack).await? {
                if !confirm()
                    .with_prompt(format!("Delete stack {}?", stack))
                    .interact()?
                {
                    bail!("Exiting as requested")
                }

                delete_stack(self.cfn_client().await?, &stack).await?;
            }
        }

        Ok(())
    }
}
