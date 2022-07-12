import Ajv, { JSONSchemaType, ValidateFunction } from "ajv";
import * as glob from "glob";
import * as fs from "fs";
import * as path from "path";
import * as child_process from "child_process";

/* These interfaces MUST match with the associated JSON schema */

/* `schemas/customer.json` */
export interface Customer {
  id: string;
  name: string;

  auth0Scope: string;
}

/* `schemas/secrets.json` */
export interface CustomerSecrets {
  secrets: {
    name: string;
    value: string;
  }[];
}

/* `schemas/cluster.json` */
export interface ClusterCloudDetailsAws {
  cloud: "AWS";
  region: string;
}

export type ClusterCloudDetails = ClusterCloudDetailsAws;
export interface Cluster {
  id: string;
  name: string;

  serverCount: number;
  adapterCount: number;
  upstreamDatabaseUriSecretName: string;

  cloudDetails: ClusterCloudDetails;
}

// The functions here are around using Ajv to do validation of the input from
// disk and apply types to the data.

// Enable strictness to make sure the schemas are valid when parsed.
const ajv = new Ajv({
  strict: true,
});

function loadSchema<T>(name: string): JSONSchemaType<T> {
  // Read a schema off disk and parse it as JSON.
  return JSON.parse(
    fs.readFileSync(path.resolve(__dirname, `../schemas/${name}.json`), "utf-8")
  ) as JSONSchemaType<T>;
}

function compileSchema<T>(name: string): ValidateFunction<T> {
  // Create the validation function which is a TypeScript type guard for the
  // generic type T using the schema loaded from schemas.
  const schema = loadSchema<T>(name);
  return ajv.compile(schema);
}

const validateCustomer = compileSchema<Customer>("customer");
const validateCustomerSecrets = compileSchema<CustomerSecrets>("secrets");
const validateCluster = compileSchema<Cluster>("cluster");

// TODO: A list of these is currently the output of the
// getCloudRegionsForCustomer function but the interface here likely needs to
// change to better accomodate what is needed to extract from the overall
// data for what is needed to generate per customer constructs.
interface CloudRegion {
  cloud: "AWS";
  region: string;
}

export class Repository {
  customers: Customer[] = [];
  clustersByCustomer: Record<string, Cluster[] | undefined> = {};
  secretsByCustomer: Record<string, CustomerSecrets | undefined> = {};

  constructor(
    customersRootPath: string | undefined = undefined,
    sopsAgeKeyFile: string | undefined = undefined
  ) {
    // Currently this fetches everything on construction. Eventually, this
    // should be lazier.

    // Allow importing from a different root for testing.
    if (customersRootPath === undefined) {
      customersRootPath = path.resolve(__dirname, "../customers");
    }

    // Use glob to get all directories with a customer.json file
    const customerJsonPathBases = glob.sync("*/customer.json", {
      cwd: customersRootPath,
    });

    for (const customerJsonPathBase of customerJsonPathBases) {
      const customerIdFromPath = path.dirname(customerJsonPathBase);
      const customerJsonPath = path.resolve(
        customersRootPath,
        customerJsonPathBase
      );
      const customerPath = path.dirname(customerJsonPath);

      const customer: unknown = JSON.parse(
        fs.readFileSync(customerJsonPath, "utf-8")
      );

      // Validate the customer JSON file matches the schemas.
      if (!validateCustomer(customer)) {
        const errors = validateCustomer.errors;
        let errorMessage = `Customer '${customerIdFromPath} customer.json file is invalid: `;
        if (!errors) {
          errorMessage += "unknown error";
        } else {
          errorMessage += errors[0].message;
        }
        throw new Error(errorMessage);
      }
      if (customer.id != customerIdFromPath) {
        throw new Error(
          `Customer '${customerIdFromPath}' customer.json file ID does not equal path name`
        );
      }
      this.customers.push(customer);

      // Copy process.env so we can override the SOPS_AGE_KEY_FILE so we can
      // have a testing key imported into the repository.
      const customerSecretsSopsExecEnv = Object.assign({}, process.env);
      if (sopsAgeKeyFile) {
        customerSecretsSopsExecEnv.SOPS_AGE_KEY_FILE = sopsAgeKeyFile;
      }

      // Instead of fetching the file off the disk directly, this runs a
      // subprocess which handles the decryption.
      const customerSecrets: unknown = JSON.parse(
        child_process.execFileSync("sops", ["-d", "secrets.json"], {
          cwd: customerPath,
          encoding: "utf-8",
          env: customerSecretsSopsExecEnv,
        })
      );

      if (!validateCustomerSecrets(customerSecrets)) {
        const errors = validateCustomerSecrets.errors;
        let errorMessage = `Customer '${customerIdFromPath} secrets.json file is invalid: `;
        if (!errors) {
          errorMessage += "unknown error";
        } else {
          errorMessage += errors[0].message;
        }
        throw new Error(errorMessage);
      }
      this.secretsByCustomer[customer.id] = customerSecrets;

      const clustersForCustomer = [];
      const customerClustersPath = path.resolve(
        customersRootPath,
        customer.id,
        "clusters"
      );

      // Parse each json file inside the clusters directory of a customer as a
      // cluster
      const clusterJsonPathBases = glob.sync("*.json", {
        cwd: customerClustersPath,
      });
      for (const clusterJsonPathBase of clusterJsonPathBases) {
        const clusterIdFromPath = path.basename(clusterJsonPathBase, ".json");
        const clusterJsonPath = path.resolve(
          customerClustersPath,
          clusterJsonPathBase
        );
        const cluster: unknown = JSON.parse(
          fs.readFileSync(clusterJsonPath, "utf-8")
        );

        if (!validateCluster(cluster)) {
          const errors = validateCluster.errors;
          let errorMessage = `Cluster definition '${clusterIdFromPath}' for '${customer.id}' is invalid: `;
          if (!errors) {
            errorMessage += "unknown error";
          } else {
            errorMessage += errors[0].message;
          }
          throw new Error(errorMessage);
        }
        if (cluster.id != clusterIdFromPath) {
          throw new Error(
            `Cluster definition '${clusterIdFromPath}' for '${customer.id}' does not have ID equal to path name`
          );
        }
        clustersForCustomer.push(cluster);
      }

      this.clustersByCustomer[customer.id] = clustersForCustomer;
    }
  }

  public getCustomers(): Customer[] {
    return this.customers;
  }

  public getClustersForCustomerId(customerId: string): Cluster[] {
    const clusters = this.clustersByCustomer[customerId];
    if (clusters) {
      return clusters;
    }
    throw new Error(`No clusters for customer '${customerId}'`);
  }

  public getCloudRegionsForCustomer(customerId: string): CloudRegion[] {
    const clusters = this.getClustersForCustomerId(customerId);
    return clusters.map((cluster) => {
      const { cloud, region } = cluster.cloudDetails;
      return { cloud, region };
    });
  }

  public getSecretsForCustomerId(customerId: string): CustomerSecrets {
    const secrets = this.secretsByCustomer[customerId];
    if (secrets) {
      return secrets;
    }
    throw new Error(`No secrets for customer '${customerId}'`);
  }
}
