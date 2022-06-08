#!/usr/bin/env python3
import os

from aws_cdk import aws_ec2 as ec2
import aws_cdk as cdk

from stacks.monitoring import MonitoringStack
from stacks.vpc import Vpc

#DISABLE_BOOTSTRAP = cdk.BootstraplessSynthesizer()
VPC_SYNTH = cdk.DefaultStackSynthesizer(generate_bootstrap_version_rule=False)
MONITORING_SYNTH = cdk.DefaultStackSynthesizer(generate_bootstrap_version_rule=False)
#DISABLE_BOOTSTRAP = cdk.LegacyStackSynthesizer()


def get_config():
    environment = app.node.try_get_context('env')
    if not environment:
        raise ValueError("Please supply an env via the CLI; i.e. \'cdk synth -c env=dev\'")

    config = app.node.try_get_context(environment)
    if not config:
        raise KeyError(f"configuration not found for environment {environment} in context")

    return config

def match_subnet_type(subnet_type):
    return getattr(ec2.SubnetType, subnet_type, ec2.SubnetType.PRIVATE_ISOLATED)

app = cdk.App()

build_config = get_config()

subnet_config = [
    ec2.SubnetConfiguration(name=s['name'],
    cidr_mask=s['cidrMask'],
    subnet_type=match_subnet_type(s['subnet_type']))
    for s in build_config['Subnets']]

vpc_stack = Vpc(app, "ReadysetVpcStack", subnets=subnet_config, synthesizer=VPC_SYNTH)

monitoring = MonitoringStack(app, "ReadysetCdkStack", vpc=vpc_stack.vpc, synthesizer=MONITORING_SYNTH)
#monitoring = MonitoringStack(app, "ReadysetCdkStack", vpc=vpc_stack.vpc)

app.synth()
