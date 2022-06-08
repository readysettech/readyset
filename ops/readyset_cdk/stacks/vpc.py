from aws_cdk import DefaultStackSynthesizer, aws_ec2 as ec2
import aws_cdk as cdk
from constructs import Construct
from aws_cdk import Stack

class Vpc(Stack):

    def __init__(self, scope: Construct, construct_id: str, subnets={}, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(self, "VPC", subnet_configuration=subnets)

