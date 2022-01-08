# DevOps Concerns

## Creating IAM Roles with Substrate

_Note this process is being deprecated as of 1/10/2022 and replaced._

### IAM Assume Role Policy

We'll create _two_ `aws_iam_policy_document` in the substrate module directory, `ops/substrate/modules`.

The first should describe the list of AWS ARNs that can assume the role we'll create later in the [IAM Role](#iam-role)
section , called the "Assume Role" policy / trust relationships. For example:

```hcl
# ops/substrate/modules/deploy-internal-cfn-template/global/iam.tf

data "aws_iam_policy_document" "internal-artifacts-access-assume-role-document" {

  statement {
    sid = ""
    effect = "Allow"
    actions = [
      "sts:AssumeRole"
    ]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::305232526136:role/buildkite-ops-Role"]
    }
  }

}
```

The second IAM policy document should describe the _minimum set of necessary permissions_ to perform a given task. For example:

```hcl
# ops/substrate/modules/deploy-internal-cfn-template/global/iam.tf

data "aws_iam_policy_document" "internal-artifacts-access-policy-role-document" {

  statement {
    sid    = ""
    effect = "Allow"
    resources = [
      "arn:aws:s3:::readysettech-cfn-internal-us-east-2",
      "arn:aws:s3:::readysettech-cfn-internal-us-east-2/*"
    ]
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:ListBucket"
    ]
  }

}
```

Note that in the example we not only specify minimal allowed actions but also confine things to a specific resource in this
case a particular S3 bucket, `readysettech-cfn-internal-us-east-2`, and its associated objects.

#### IAM Role

Now we'll create the IAM Role that will be assumed by the ARNs we defined in [IAM Assume Role Policy](#iam-assume-role-policy)
section.

```hcl
# ops/substrate/modules/deploy-internal-cfn-template/global/iam.tf

resource "aws_iam_role" "internal-artifacts-write" {

  name = "InternalArtifactsWrite"
  assume_role_policy = data.aws_iam_policy_document.internal-artifacts-access-assume-role-document.json

}
```

#### IAM Role Policy

Next we need to associate the IAM role policy that describes allowed actions we made earlier with the IAM Role we just created.

```hcl
# ops/substrate/modules/deploy-internal-cfn-template/global/iam.tf

resource "aws_iam_role_policy" "internal-artifacts-write-role-policy" {
  policy = data.aws_iam_policy_document.internal-artifacts-access-policy-role-document.json
  role   = aws_iam_role.internal-artifacts-write.id
  name   = "InternalArtifactsWriteRolePolicy"
}
```

#### Outputs

While not strictly necessary outputs can be very helpful for aiding manual steps required by some processes.
Outputting the ARN of the IAM Role we created is one example of a helpful output.

```hcl
# ops/substrate/modules/deploy-internal-cfn-template/global/iam.tf

output "internal-artifacts-write-arn" {
  value = aws_iam_role.internal-artifacts-write.arn
  description = "ARN of the Internal Artifacts Write IAM Role"
}
```

### IAM Assume Role Policy

rotating_light: **ONLY NECESSARY IF DOING CROSS ACCOUNT ROLE ASSUMPTION** :rotating_light:

_This section requires manual provisioning via the AWS console_

