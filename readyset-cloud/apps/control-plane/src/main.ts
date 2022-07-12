import { Construct } from "constructs";
import { App, TerraformStack } from "cdktf";
import { AwsProvider } from "../.gen/providers/aws";

import type { Cluster } from "data-plane";
import { Repository } from "data-plane";

interface CustomerVpcStackOptions {
  region: string;
}

interface CustomerEksClusterStackOptions {
  region: string;
}

interface ReadySetKubernetesDeploymentStackOptions {
  serverCount: number;
  adapterCount: number;
}

function vpcOptionsForCustomer(
  customerId: string,
  region: string
): CustomerVpcStackOptions {
  return {
    region: region,
  };
}

function eksOptionsForCustomer(
  customerId: string,
  region: string
): CustomerEksClusterStackOptions {
  return {
    region: region,
  };
}

function readySetKubernetesDeploymentOptionsForCluster(
  cluster: Cluster
): ReadySetKubernetesDeploymentStackOptions {
  return {
    adapterCount: cluster.adapterCount,
    serverCount: cluster.serverCount,
  };
}

export class CustomerVpcStack extends TerraformStack {
  constructor(scope: Construct, id: string, options: CustomerVpcStackOptions) {
    super(scope, id);
    const { region } = options;
    new AwsProvider(this, "aws", {
      region,
    });
  }
}

export class CustomerEksClusterStack extends TerraformStack {
  constructor(
    scope: Construct,
    id: string,
    options: CustomerEksClusterStackOptions
  ) {
    super(scope, id);
    const { region } = options;
    new AwsProvider(this, "aws", {
      region,
    });
  }
}

class ReadysetKubernetesDeploymentStack extends TerraformStack {
  constructor(
    scope: Construct,
    id: string,
    options: ReadySetKubernetesDeploymentStackOptions
  ) {
    super(scope, id);
  }
}

const app = new App();
const repository = new Repository();

for (const customer of repository.getCustomers()) {
  const customerId = customer.id;
  const cloudRegions = repository.getCloudRegionsForCustomer(customerId);
  for (const { cloud, region } of cloudRegions) {
    const vpcOptions = vpcOptionsForCustomer(customerId, region);
    new CustomerVpcStack(app, `${customerId}-vpc`, vpcOptions);
    const eksOptions = eksOptionsForCustomer(customerId, region);
    new CustomerEksClusterStack(app, `${customerId}-eks`, eksOptions);
  }

  for (const cluster of repository.getClustersForCustomerId(customerId)) {
    const clusterId = cluster.id;

    const deploymentOptions =
      readySetKubernetesDeploymentOptionsForCluster(cluster);
    new ReadysetKubernetesDeploymentStack(
      app,
      `${clusterId}-${customerId}-deployment`,
      deploymentOptions
    );
  }
}

app.synth();
