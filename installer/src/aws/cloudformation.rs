use std::borrow::{Borrow, Cow};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use aws_sdk_cloudformation as cfn;
use cfn::client::fluent_builders::CreateStack;
use cfn::model::{Stack, StackStatus};
use cfn::SdkError;
use console::{style, Emoji};
use regex::Regex;
use reqwest::IntoUrl;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tokio::time::sleep;

use crate::console::{confirm, input, password, spinner, GREEN_CHECK};

use super::cfn_parameter;

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct TemplateParameter {
    pub(crate) description: Option<String>,

    #[serde(rename = "Type")]
    pub(crate) type_: String,

    pub(crate) default: Option<String>,

    pub(crate) allowed_pattern: Option<String>,

    pub(crate) min_length: Option<usize>,

    pub(crate) max_length: Option<usize>,

    pub(crate) constraint_description: Option<String>,

    /// Unfortunately this has to be a value instead of a proper boolean because sometimes it's in
    /// cloudformation as the strings "True" or "False" instead of a boolean, and sometimes it's a
    /// boolean
    pub(crate) no_echo: Option<serde_yaml::Value>,
}

impl TemplateParameter {
    fn no_echo(&self) -> bool {
        match &self.no_echo {
            None => false,
            Some(serde_yaml::Value::Bool(b)) => *b,
            Some(serde_yaml::Value::String(s)) if s == "True" => true,
            Some(serde_yaml::Value::String(s)) if s == "False" => false,
            _ => false,
        }
    }

    fn validate(&self, value: &str) -> Result<(), String> {
        let (valid, default_msg) = match self.type_.as_str() {
            "Number" => (
                value.parse::<isize>().is_ok(),
                Cow::Borrowed("Must be a number"),
            ),
            "List<Number>" => (
                value.split(',').all(|n| n.parse::<isize>().is_ok()),
                Cow::Borrowed("Must be a comma-separated list of numbers"),
            ),
            _ => {
                if let Some(min_length) = self.min_length.filter(|ml| value.len() < *ml) {
                    (
                        false,
                        Cow::Owned(format!("Must be at least {} characters long", min_length)),
                    )
                } else if let Some(max_length) = self.max_length.filter(|ml| value.len() > *ml) {
                    (
                        false,
                        Cow::Owned(format!("Must be at most {} characters long", max_length)),
                    )
                } else if let Some(allowed_pattern) = self
                    .allowed_pattern
                    .as_ref()
                    .filter(|pat| Regex::new(pat).map_or(false, |re| !re.is_match(value)))
                {
                    (
                        false,
                        Cow::Owned(format!("Must match pattern {}", allowed_pattern)),
                    )
                } else {
                    return Ok(());
                }
            }
        };
        if valid {
            Ok(())
        } else {
            Err(self
                .constraint_description
                .clone()
                .unwrap_or_else(|| default_msg.into_owned()))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ParameterGroupLabel {
    default: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct ParameterGroup {
    label: ParameterGroupLabel,
    parameters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct CloudFormationInterface {
    pub(crate) parameter_groups: Vec<ParameterGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TemplateMetadata {
    #[serde(rename = "AWS::CloudFormation::Interface")]
    interface: CloudFormationInterface,
}

/// Subset of the structure of a cloudformation template, used to deserialize parameter groups and
/// parameters to allow us to prompt the user for parameter values during the installation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct Template {
    metadata: TemplateMetadata,
    parameters: HashMap<String, TemplateParameter>,
}

impl Template {
    /// Download and deserialize a cloudformation template from the given URL
    pub(crate) async fn download<U: IntoUrl>(url: U) -> Result<Self> {
        let text = reqwest::get(url).await?.text().await?;
        Ok(serde_yaml::from_str(&text)?)
    }
}

/// A description of a to-be-deployed cloudformation stack, with methods to prompt the user to
/// modify the config
pub(crate) struct StackConfig {
    template: Template,
    parameters: HashMap<String, String>,
    /// Fields that cannot be modified by the user
    non_modifiable_parameters: HashSet<String>,
}

impl StackConfig {
    pub(crate) fn new(template: Template) -> Self {
        StackConfig {
            template,
            parameters: Default::default(),
            non_modifiable_parameters: Default::default(),
        }
    }

    pub(crate) async fn from_url<U: IntoUrl>(url: U) -> Result<Self> {
        let template = Template::download(url).await?;
        Ok(Self::new(template))
    }

    pub(crate) fn with_non_modifiable_parameter<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        let key = key.into();
        self.parameters.insert(key.clone(), value.into());
        self.non_modifiable_parameters.insert(key);
        self
    }

    /// Returns the current value for the given parameter, either overridden or the default value in
    /// the template.
    pub(crate) fn parameter_value<P>(&self, parameter: &P) -> Option<&str>
    where
        String: Borrow<P>,
        P: ?Sized + Hash + Eq,
    {
        self.parameters
            .get(parameter)
            .or_else(|| {
                self.template
                    .parameters
                    .get(parameter)
                    .and_then(|p| p.default.as_ref())
            })
            .map(|s| s.as_str())
    }

    /// Prompt the user for a new value for the current parameter, and set the value for that
    /// parameter in self.
    ///
    /// It is the responsibility of the caller to ensure that the parameter is modifiable
    pub(crate) fn prompt_for_parameter(&mut self, parameter_name: String) -> Result<()> {
        let parameter = self
            .template
            .parameters
            .get(&parameter_name)
            .ok_or_else(|| anyhow!("Parameter {} not found", parameter_name))?;
        // TODO(grfn): Figure out how to fit the description in here
        let value = if parameter.no_echo() {
            password().with_prompt(&parameter_name).interact()?
        } else {
            let mut input = input();
            input
                .with_prompt(&parameter_name)
                .validate_with(|input: &String| parameter.validate(input));
            if let Some(default) = parameter.default.clone() {
                input.default(default);
            }
            input.interact_text()?
        };

        self.parameters.insert(parameter_name, value);

        Ok(())
    }

    pub(crate) fn apply_to_create_stack(self, mut create_stack: CreateStack) -> CreateStack {
        for (key, value) in self.parameters {
            create_stack = create_stack.parameters(cfn_parameter(key, value));
        }
        create_stack
    }

    pub(crate) fn describe_non_modifiable_config(&self) {
        for param in self.modifiable_parameters() {
            if let Some(val) = self.parameter_value(&param) {
                eprintln!("   {}: {}", style(param).bold(), style(val).blue());
            }
        }
    }

    pub(crate) fn modify_config(&mut self) -> Result<()> {
        for param in self.modifiable_parameters() {
            self.prompt_for_parameter(param)?;
        }

        Ok(())
    }

    fn modifiable_parameters(&self) -> Vec<String> {
        self.parameters
            .keys()
            .chain(self.template.parameters.keys())
            .filter(|param| !self.non_modifiable_parameters.contains(*param))
            .collect::<HashSet<_>>()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Prompt the user for all required but not currently set parameters, and set the values for
    /// those parameters in self.
    ///
    /// If any required parameter is currently set but not modifiable, returns an error.
    pub(crate) fn prompt_for_required_parameters(&mut self) -> Result<()> {
        let required_parameters = self
            .template
            .parameters
            .iter()
            .filter(|(k, v)| v.default.is_none() && !self.parameters.contains_key(*k))
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        for param in required_parameters {
            self.prompt_for_parameter(param)?;
        }
        Ok(())
    }
}

pub(crate) async fn describe_stack(
    cfn_client: &cfn::Client,
    stack_name: &str,
) -> Result<Option<Stack>> {
    let res = cfn_client
        .describe_stacks()
        .stack_name(stack_name)
        .send()
        .await;
    match res {
        Ok(r) => Ok(r.stacks.unwrap_or_default().into_iter().next()),
        Err(SdkError::ServiceError { err, .. })
            if err
                .message()
                .iter()
                .any(|msg| msg.contains("does not exist")) =>
        {
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) async fn delete_stack(cfn_client: &cfn::Client, stack_name: &str) -> Result<()> {
    let delete_desc = format!("CloudFormation stack {}", style(stack_name).bold());
    let delete_pb = spinner().with_message(format!("Deleting {}", delete_desc));
    cfn_client
        .delete_stack()
        .stack_name(stack_name)
        .send()
        .await?;
    loop {
        match describe_stack(cfn_client, stack_name).await? {
            None => break,
            Some(stack) if stack.stack_status == Some(StackStatus::DeleteComplete) => break,
            Some(stack) => {
                if let Some(status) = stack.stack_status() {
                    delete_pb.set_message(format!(
                        "{} (Current status: {})",
                        delete_desc,
                        style(status.as_str()).blue()
                    ))
                }
            }
        }
    }
    delete_pb.finish_with_message(format!("{}Deleted {}", *GREEN_CHECK, delete_desc));

    Ok(())
}

pub(crate) async fn deploy_stack(
    cfn_client: &cfn::Client,
    stack_name: &str,
    operation: CreateStack,
) -> Result<Stack> {
    let do_create = if let Some(existing_stack) = describe_stack(cfn_client, stack_name).await? {
        let unhealthy = matches!(
            existing_stack.stack_status,
            Some(
                StackStatus::CreateFailed
                    | StackStatus::DeleteFailed
                    | StackStatus::RollbackComplete
                    | StackStatus::RollbackFailed
                    | StackStatus::UpdateFailed
            )
        );
        let status_desc = style(
            existing_stack
                .stack_status
                .clone()
                .unwrap()
                .as_str()
                .to_owned(),
        );
        println!(
            "Stack {} already exists (status: {})",
            style(stack_name).bold(),
            if unhealthy {
                status_desc.red()
            } else {
                status_desc.blue()
            }
        );

        if unhealthy {
            if confirm()
                .with_prompt("Would you like to delete and re-create the stack?")
                .default(true)
                .interact()?
            {
                delete_stack(cfn_client, stack_name).await?;
                true
            } else {
                bail!("Please fix any issues with the stack and re-run the installer");
            }
        } else if matches!(
            existing_stack.stack_status,
            Some(StackStatus::CreateComplete | StackStatus::UpdateComplete)
        ) {
            return Ok(existing_stack);
        } else {
            false
        }
    } else {
        true
    };

    if do_create {
        let create_pb = spinner().with_message(format!(
            "Creating CloudFormation stack {}",
            style(stack_name).bold()
        ));
        operation.send().await?;
        create_pb.finish_with_message(format!(
            "{}Created CloudFormation stack {}",
            *GREEN_CHECK,
            style(stack_name).bold()
        ));
    }

    let ready_pb_desc = format!(
        "Waiting for stack to have status {}",
        style(StackStatus::CreateComplete.as_str()).blue()
    );
    let ready_pb = spinner().with_message(ready_pb_desc.clone());
    loop {
        let stack = describe_stack(cfn_client, stack_name)
            .await?
            .ok_or_else(|| anyhow!("CloudFormation stack went away!"))?;

        match stack.stack_status {
            Some(StackStatus::CreateComplete) => {
                ready_pb.finish_with_message(format!(
                    "{}Stack {} finished creating",
                    *GREEN_CHECK,
                    style(&stack_name).bold()
                ));
                break Ok(stack);
            }
            Some(
                status
                @
                (StackStatus::CreateFailed
                | StackStatus::DeleteFailed
                | StackStatus::RollbackComplete
                | StackStatus::RollbackFailed
                | StackStatus::UpdateFailed),
            ) => {
                ready_pb.abandon_with_message(format!(
                    "{} Stack entered non-successful status {}",
                    style(Emoji("❌ ", "X ")).red(),
                    style(status.as_str()).red()
                ));
                bail!("Stack creation failed");
            }
            status => {
                if let Some(status) = status {
                    ready_pb.set_message(format!(
                        "{} (Current status: {})",
                        ready_pb_desc,
                        style(status.as_str()).blue()
                    ));
                }
                sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn deserialize_template_parameter() {
        let input = "
Description: Database name
Type: String
MinLength: 1
MaxLength: 64
AllowedPattern: '[a-zA-Z][a-zA-Z0-9]*'
ConstraintDescription: Must begin with a letter and contain only alphanumeric characters.
";
        let template_parameter = serde_yaml::from_str::<TemplateParameter>(input).unwrap();
        assert_eq!(
            template_parameter,
            TemplateParameter {
                description: Some("Database name".to_owned()),
                type_: "String".to_owned(),
                default: None,
                min_length: Some(1),
                max_length: Some(64),
                allowed_pattern: Some("[a-zA-Z][a-zA-Z0-9]*".to_owned()),
                constraint_description: Some(
                    "Must begin with a letter and contain only alphanumeric characters.".to_owned()
                ),
                no_echo: None,
            }
        );
    }

    #[test]
    fn deserialize_readyset_mysql_super_template() {
        let mut path = PathBuf::from("..").canonicalize().unwrap();
        path.push("ops/cfn/templates/readyset-mysql-super-template.yaml");
        let file = File::open(path).unwrap();
        let template_res = serde_yaml::from_reader::<_, Template>(file);
        assert!(template_res.is_ok(), "{}", template_res.err().unwrap());
    }
}
