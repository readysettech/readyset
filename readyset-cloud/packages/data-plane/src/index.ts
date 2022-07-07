export interface Customer {
  id: string;
  name: string;
  auth0Scope: string;
}

export interface AwsCloudDetails {
  kind: "AWS";
  region: string;
  instanceType: string;
}

type CloudDetails = AwsCloudDetails;

export interface Cluster {
  id: string;
  customer: Customer;

  serverCount: number;
  adapterCount: number;
  upstreamDatabaseUri: string;

  cloudDetails: CloudDetails;
}

const CUSTOMER_FOO: Customer = {
  id: "foo",
  name: "foo",
  auth0Scope: "foo",
};

const CLUSTER_BAR: Cluster = {
  id: "bar",
  customer: CUSTOMER_FOO,

  serverCount: 1,
  adapterCount: 3,
  upstreamDatabaseUri: "mysql://username:password@host:port/db",

  cloudDetails: {
    kind: "AWS",
    region: "us-east-2",
    instanceType: "c5.xlarge",
  },
};

export const ALL_CLUSTERS = [CLUSTER_BAR];

export interface CloudRegion {
  cloud: "AWS";
  region: string;
}

export function getCloudsAndRegionsByCustomerId(): Map<
  string,
  Set<CloudRegion>
> {
  const ret = new Map<string, Set<CloudRegion>>();
  for (const cluster of ALL_CLUSTERS) {
    const key = cluster.customer.id;
    const value: CloudRegion = {
      cloud: cluster.cloudDetails.kind,
      region: cluster.cloudDetails.region,
    };
    let current = ret.get(key);
    if (current === undefined) {
      current = new Set();
      ret.set(key, current);
    }
    current.add(value);
  }
  return ret;
}
