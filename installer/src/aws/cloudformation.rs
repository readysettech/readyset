use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use aws_sdk_cloudformation as cfn;
use cfn::client::fluent_builders::CreateStack;
use cfn::model::{Stack, StackStatus};
use cfn::SdkError;
use console::{style, Emoji};
use tokio::time::sleep;

use crate::console::{spinner, GREEN_CHECK};

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

pub(crate) async fn deploy_stack(
    cfn_client: &cfn::Client,
    stack_name: &str,
    operation: CreateStack,
) -> Result<Stack> {
    if let Some(existing_stack) = describe_stack(cfn_client, stack_name).await? {
        println!(
            "Stack {} already exists (status: {})",
            style(stack_name).bold(),
            style(existing_stack.stack_status.clone().unwrap().as_str()).blue()
        );
        if matches!(
            existing_stack.stack_status,
            Some(
                StackStatus::CreateComplete
                    | StackStatus::UpdateComplete
                    | StackStatus::RollbackComplete
            )
        ) {
            return Ok(existing_stack);
        }
    } else {
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
