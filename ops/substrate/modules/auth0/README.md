# Auth0 Module

This module contains Auth0 assets used for customer identity management.

## Bootstrapping Phase

### Provision an Auth0 App for Terraform

We need to manually set up an Auth0 app that allows Terraform to manage the Auth0 API before we can use the Auth0
Terraform provider.

* Login to Auth0 using the credentials stored in 1Password (username is `services@readyset.io`).
* Go to the Auth0 dashboard.
* Click on the Applications section on the left.
* Click "Create Application" on the right.
* In the pop-up prompt name your app something like "Terraform API Management" or similar and select
  "Machine to Machine" as the application type. When finished click the blue "Create" at the bottom center of the
  prompt.
* On the next screen under "scopes" select "all" so Terraform can manage all aspects of the Auth0 API. Click the
  blue "Authorize" button in the bottom center of the prompt.
* You'll be taken to the information page for the newly created application. Go to the "Settings" tab and take note of
  the **Domain**, **Client ID**, and **Client Secrets** variables; we'll need these variables for the
  Terraform Auth0 Provider.
* (Optional) Under Settings => Application Properties => Application Logo set the logo to something that will make
  the Terraform Management App distinguishable at a glance.
  The official [Terraform logo](https://www.datocms-assets.com/2885/1620155116-brandhcterraformverticalcolor.svg)
  is a good place to start.

### Configure Auth0 Terraform Provider

We need to pass the **Domain**, **Client ID**, and **Client Secrets** variables we noted in the prior steps
to Terraform so it can access the Auth0 API. We also need to specify a password for a superadmin user and an API
identifier.

If we wanted to pass the variables as a variables file we'd create a new file called `.tfvars` or similar.

```hcl
# .tfvars
auth0_domain                = "<domain-from-prior-step>.us.auth0.com"
auth0_client_id             = "<client-id-from-prior-step>"
auth0_client_secret         = "<client-secret-from-prior-step>"
auth0_admin_user_password   = "<admin-account-password>"
auth0_api_identifier        = "<root-url>"
auth0_admin_user_password   = "<randomly-generated-strong-password>"
auth0_auditor_user_password = "<randomly-generated-strong-password>"
callback_url                = "<protocol>:<root-url>/<callback-slug>"
logout_url                  = "<protocol>:<root-url>/<logout-slug>"
```

To use this variable file during a plan or apply step we'd use the `-var-file` flag as demonstrated below:

```bash
terraform plan -var-file=.tfvars -out tfplan
```

There's an example terraform variable file provided at `global/.tfvars.example`.

You can also export the aforementioned variables using the format `TF_VAR_<variable-name>` to set the Terraform variable
with name `<variable-name>`.

```bash
$ export TF_VAR_auth0_domain={"<domain-from-prior-step>.us.auth0.com"}
$ export TF_VAR_auth0_client_id={"<client-id-from-prior-step>"}
$ export TF_VAR_auth0_client_secret={"<client-secret-from-prior-step>"}
$ export TF_VAR_auth0_admin_user_password={"<admin-account-password>"}
$ export TF_VAR_auth0_api_identifier={"<root-url>"}
$ auth0_admin_user_password={"<randomly-generated-strong-password>"}
$ auth0_auditor_user_password={"<randomly-generated-strong-password>"}
$ callback_url={"<protocol>:<root-url>/<callback-slug>"}
$ logout_url={"<protocol>:<root-url>/<logout-slug>"}
```