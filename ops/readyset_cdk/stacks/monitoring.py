from readyset_constructs.services import AutoScalingApplicationLoadBalancer

from aws_cdk import (
    # Duration,
    CfnParameter,
    Stack
    # aws_sqs as sqs,
)

from constructs import Construct

pattern = '^(\w)[5-7].*'
stack_id = "Monitoring"

class MonitoringStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, vpc: None, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        asgalb = AutoScalingApplicationLoadBalancer(self, stack_id, vpc=vpc)

        foo = CfnParameter(
            self,
            "ReadySetAdapterInstanceType",
            allowed_pattern=pattern,
            type="String",
            default="m5.large",
            constraint_description="Instance type must be 5th, 6th, or 7th gen"
        )