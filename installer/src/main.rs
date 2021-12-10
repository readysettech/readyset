use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::{env, iter};

use ::console::{style, Emoji};
use anyhow::{anyhow, bail, Result};
use aws_sdk_ec2 as ec2;
use aws_sdk_rds as rds;
use aws_types::credentials::future::ProvideCredentials as ProvideCredentialsFut;
use aws_types::credentials::{CredentialsError, ProvideCredentials};
use aws_types::region::Region;
use aws_types::Credentials;
use clap::Parser;
use directories::ProjectDirs;
use ec2::model::{KeyType, Subnet};
use futures::stream::{FuturesUnordered, TryStreamExt};
use indicatif::MultiProgress;
use ipnet::Ipv4Net;
use launchpad::display::EnglishList;
use rds::model::{ApplyMethod, DbInstance, DbParameterGroup, Parameter};
use rusoto_credential::ProfileProvider;
use tokio::fs::{DirBuilder, OpenOptions};
use tokio::io::AsyncWriteExt;

mod aws;
mod deployment;
#[macro_use]
mod console;

use crate::aws::filter;
use crate::console::{confirm, input, select, spinner, GREEN_CHECK};
pub use crate::deployment::Deployment;
use crate::deployment::{CreateNew, Engine, Existing, MaybeExisting, RdsDb};

/// List of regions where we deploy AMIs.
///
/// Should match `destination_regions` in `//ops/image-deploy/locals.pkr.hcl`
const REGIONS: &[&str] = &["us-east-1", "us-east-2", "us-west-2"];

/// Minimum number of availability zones in which we can deploy a cluster
const MIN_AVAILABILITY_ZONES: usize = 3;

/// Install and configure a ReadySet cluster in AWS
#[derive(Parser)]
struct Options {
    /// Directory to store state between runs. Defaults to `$XDG_STATE_HOME/readyset`.
    state_directory: Option<PathBuf>,
}

impl Options {
    fn state_directory(&self) -> Result<Cow<Path>> {
        if let Some(state_directory) = &self.state_directory {
            return Ok(Cow::Borrowed(state_directory));
        }

        let project_dirs = ProjectDirs::from("io", "readyset", "ReadySet").ok_or_else(|| {
            anyhow!("Could not determine HOME directory, and --state-directory not passed")
        })?;
        Ok(Cow::Owned(project_dirs.data_dir().to_owned()))
    }
}

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

struct Installer {
    options: Options,
    deployment: Deployment,

    // Runtime state
    aws_config: Option<aws_config::Config>,
    ec2_client: Option<ec2::Client>,
    rds_client: Option<rds::Client>,
}

impl Installer {
    fn new(options: Options, deployment: Deployment) -> Self {
        Self {
            options,
            deployment,
            aws_config: None,
            ec2_client: None,
            rds_client: None,
        }
    }

    /// Save this installer's deployment to the configured state directory
    pub async fn save(&self) -> Result<()> {
        self.deployment
            .save_to_directory(self.options.state_directory()?)
            .await
    }

    /// Run the install process, picking up where the user left off if necessary
    pub async fn run(&mut self) -> Result<()> {
        self.save().await?;

        self.load_aws_config().await?;
        self.save().await?;

        if let Some(vpc_id) = &self.deployment.vpc_id {
            match vpc_id {
                Existing(vpc_id) => println!("Using existing AWS VPC: {}", style(vpc_id).bold()),
                CreateNew => println!("Deploying to a {} AWS VPC", style("new").bold()),
            }
        } else {
            self.prompt_for_vpc().await?;
        };
        self.save().await?;

        if let Existing(vpc_id) = self.deployment.vpc_id.clone().unwrap() {
            match &self.deployment.subnet_ids {
                Some(subnet_ids) => println!(
                    "Deploying to subnets: {}",
                    subnet_ids
                        .iter()
                        .map(|subnet_id| style(subnet_id).bold())
                        .into_and_list()
                ),
                None => self.validate_vpc(vpc_id).await?,
            }
        }

        if let Some(rds_db) = &self.deployment.rds_db {
            match &rds_db.db_id {
                CreateNew => println!(
                    "Will create a new {} RDS database instance",
                    style(rds_db.engine).bold()
                ),
                Existing(rds_db_id) => println!(
                    "Deploying in front of existing RDS database instance: {}",
                    style(rds_db_id).bold()
                ),
            }
        } else {
            self.prompt_for_rds_database().await?;
        }
        self.save().await?;

        let rds_db = self.deployment.rds_db.clone().unwrap();
        if let Existing(db_id) = rds_db.db_id {
            self.validate_rds_database(db_id, rds_db.engine).await?;
        }

        if let Some(key_pair_name) = &self.deployment.key_pair_name {
            success!("Using SSH key pair: {}", key_pair_name)
        } else {
            self.create_and_install_key_pair().await?;
        }
        self.save().await?;

        Ok(())
    }

    async fn create_and_install_key_pair(&mut self) -> Result<()> {
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
                .with_prompt("Path to save key pair to")
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

        self.deployment.key_pair_name = Some(key_pair_name);

        success!("Configured SSH key");

        Ok(())
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
        let vpc_cidr: Ipv4Net = if let Some(vpc) = vpcs.vpcs.unwrap_or_default().first() {
            vpc.cidr_block()
                .ok_or_else(|| anyhow!("VPC with id {} doesn't have any CIDR blocks", vpc_id))?
                .parse()?
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

                match self.deployment.vpc_id.as_ref().unwrap() {
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
                            .first()
                            .unwrap()
                            .cidr_block()
                            .ok_or_else(|| {
                                anyhow!("VPC with id {} doesn't have any CIDR blocks!", vpc_id)
                            })?
                            .parse()?
                    }
                }
            } else {
                bail!("Could not successfully access the configured VPC");
            }
        };

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

        // Just use the first subnet we find in each AZ, for now.
        //
        // Later, we can modify this to prefer private / public subnets
        let existing_subnet_ids = subnets_by_az
            .values()
            .map(|subnets| subnets.first().unwrap().subnet_id.clone().unwrap())
            .collect::<Vec<_>>();

        if existing_subnet_ids.len() < MIN_AVAILABILITY_ZONES {
            let other_azs = self
                .ec2_client()
                .await?
                .describe_availability_zones()
                .filters(filter(
                    "region-name",
                    self.deployment.aws_region.as_ref().unwrap(),
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
                self.deployment.subnet_ids = Some(subnet_ids);
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
            self.deployment.subnet_ids = Some(existing_subnet_ids)
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

                    let rds_db = self.deployment.rds_db.as_ref().unwrap();
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
            let existing_parameter_group = self
                .rds_client()
                .await?
                .describe_db_parameter_groups()
                .db_parameter_group_name(
                    db_instance
                        .db_parameter_groups
                        .unwrap_or_default()
                        .first()
                        .cloned()
                        .ok_or_else(|| {
                            anyhow!(
                                "Could not find existing db parameter group for RDS database {}",
                                db_id
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
                        db_id
                    )
                })?;

            Some(
                self.create_mysql_parameter_group(existing_parameter_group, parameters)
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

        let reboot_db_desc = format!("RDS database instance {}", style(&db_id).bold());
        let reboot_db_pb = spinner().with_message(format!("Rebooting {}", &reboot_db_desc));
        reboot_db_pb.enable_steady_tick(50);
        self.rds_client()
            .await?
            .reboot_db_instance()
            .db_instance_identifier(&db_id)
            .send()
            .await?;
        reboot_db_pb.finish_with_message(format!("{}Rebooted {}", *GREEN_CHECK, reboot_db_desc));

        Ok(())
    }

    /// Create a new MySQL parameter group based on the given existing parameter group, and with the
    /// given set of parameters, except with `binlog_format` set to `ROW`.
    ///
    /// Returns the `parameter_group_name` of the created db parameter group
    async fn create_mysql_parameter_group(
        &mut self,
        existing_parameter_group: DbParameterGroup,
        parameters: Vec<Parameter>,
    ) -> Result<String> {
        // first, check if a parameter group named "readyset-mysql" already exists
        let existing = self
            .rds_client()
            .await?
            .describe_db_parameter_groups()
            .db_parameter_group_name("readyset-mysql")
            .send()
            .await?
            .db_parameter_groups
            .unwrap_or_default();
        let (parameter_group_name, do_create) = if existing.is_empty() {
            (Cow::Borrowed("readyset-mysql"), true)
        } else {
            println!(
                "Found an existing RDS DB parameter group named {}",
                style("readyset-mysql").bold()
            );
            if confirm()
                .with_prompt(
                    "Would you like to update that parameter group to set the correct parameters?",
                )
                .interact()?
            {
                (Cow::Borrowed("readyset-mysql"), false)
            } else {
                let parameter_group_name = input()
                    .with_prompt("Please enter a name for a DB parameter group to create")
                    .interact()?;
                (Cow::Owned(parameter_group_name), true)
            }
        };

        let (verbing, verbed) = if do_create {
            ("Creating new", "Created new")
        } else {
            ("Updating", "Updated")
        };
        let pb_desc = format!(
            "RDS parameter group {} based on {} with {}={}",
            style("readyset").bold(),
            style(existing_parameter_group.db_parameter_group_name().unwrap()).bold(),
            style("binlog_format").blue(),
            style("ROW").blue()
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
                .description("Automatically-created DB parameter group for ReadySet with MySQL")
                .send()
                .await?
                .db_parameter_group
                .unwrap();
        }

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

        self.rds_client()
            .await?
            .modify_db_parameter_group()
            .db_parameter_group_name(parameter_group_name.as_ref())
            .set_parameters(Some(
                parameters
                    .into_iter()
                    .filter(|param| {
                        param.parameter_name() != Some("binlog_format")
                            && !default_parameters.contains(&(
                                param.parameter_name.clone(),
                                param.parameter_value.clone(),
                            ))
                    })
                    .chain(iter::once(
                        Parameter::builder()
                            .parameter_name("binlog_format")
                            .parameter_value("ROW")
                            .apply_method(ApplyMethod::Immediate)
                            .build(),
                    ))
                    .collect::<Vec<_>>(),
            ))
            .send()
            .await?;

        parameter_group_pb.finish_with_message(format!("{}{} {}", *GREEN_CHECK, verbed, &pb_desc,));

        Ok(parameter_group_name.into_owned())
    }

    async fn validate_rds_database_postgresql(
        &mut self,
        _db_id: String,
        _db_instance: DbInstance,
    ) -> Result<()> {
        bail!("Sorry, the installer doesn't support PostgreSQL databases yet");
    }

    async fn prompt_for_rds_database(&mut self) -> Result<()> {
        let rds_db = if let Existing(vpc_id) = self.deployment.vpc_id.clone().unwrap() {
            if confirm()
                .with_prompt(
                    "Would you like to deploy in front of an existing RDS database instance?",
                )
                .default(false)
                .interact()?
            {
                let mut instances = aws::rds_dbs_in_vpc(self.rds_client().await?, &vpc_id).await?;
                if instances.is_empty() {
                    bail!("No RDS database instances found in {}", vpc_id);
                }
                println!(
                    "Found {} RDS database instances in {}",
                    style(instances.len()).bold(),
                    style(&vpc_id).bold()
                );
                let idx = select()
                    .with_prompt("Which RDS database instance would you like to use?")
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
                Some(RdsDb {
                    db_id: Existing(
                        instance
                            .db_instance_identifier
                            .ok_or_else(|| anyhow!("RDS instance missing identifier"))?,
                    ),
                    engine,
                })
            } else {
                println!("OK, I'll create a new RDS database instance in {}", vpc_id);
                None
            }
        } else {
            None
        }
        .map::<Result<_>, _>(Ok)
        .unwrap_or_else(|| {
            let engine =
                Engine::select("Which database engine should I create the database with?")?;

            Ok(RdsDb {
                db_id: CreateNew,
                engine,
            })
        })?;

        self.deployment.rds_db = Some(rds_db);

        Ok(())
    }

    async fn prompt_for_vpc(&mut self) -> Result<MaybeExisting<&str>> {
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
                style(self.deployment.aws_region.as_ref().unwrap()).bold()
            );
            let idx = select()
                .with_prompt("Which VPC should we deploy to?")
                .items(&vpc_names)
                .interact()?;

            Existing(vpcs[idx].0.to_owned())
        } else {
            println!("OK, we'll create a new AWS VPC as part of the deployment process");
            CreateNew
        };

        Ok(self.deployment.vpc_id.insert(vpc_id).as_deref())
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

    async fn aws_config(&mut self) -> Result<&aws_config::Config> {
        if self.aws_config.is_none() {
            self.load_aws_config().await?;
        }
        Ok(self.aws_config.as_ref().unwrap())
    }

    async fn load_aws_config(&mut self) -> Result<&aws_config::Config> {
        let mut loader = aws_config::from_env();

        let profile =
            if let Some(aws_credentials_profile) = &self.deployment.aws_credentials_profile {
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

        let region = if let Some(aws_region) = &self.deployment.aws_region {
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
            "We'll use your AWS credentials (stored in {}) throughout the install process.",
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

        Ok(self.deployment.aws_credentials_profile.insert(profile))
    }

    fn prompt_for_aws_region(&mut self) -> Result<&str> {
        let mut prompt = select();
        prompt.with_prompt("Which AWS region should we deploy to?");
        prompt.items(REGIONS);

        if let Ok(default_region) = env::var("AWS_DEFAULT_REGION") {
            if let Some(idx) = REGIONS.iter().position(|r| r == &default_region) {
                prompt.default(idx);
            }
        }

        let idx = prompt.interact()?;

        Ok(self.deployment.aws_region.insert(REGIONS[idx].to_owned()))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let options = Options::parse();

    println!("\n{}\n", style("Welcome to the ReadySet Installer!").bold());

    DirBuilder::new()
        .recursive(true)
        .create(options.state_directory()?)
        .await?;

    let deployment = deployment::create_or_load_existing(options.state_directory()?).await?;
    let mut installer = Installer::new(options, deployment);

    installer.run().await
}
