data "aws_iam_policy_document" "developer" {
  # Protected roles
  statement {
    effect = "Deny"

    actions = [
      "iam:DeleteRole",
      "iam:DeleteRolePolicy",
      "iam:DetachRolePolicy",
      "iam:UpdateRole",
    ]

    resources = [
      "arn:aws:iam::*:role/Administrator",
      "arn:aws:iam::*:role/Developer",
      "arn:aws:iam::*:role/OrganizationAdministrator",
      "arn:aws:iam::*:role/OrganizationReader",
      "arn:aws:iam::*:role/secureframe-scan",
    ]
  }

  # Limit regions
  statement {
    effect    = "Deny"
    actions   = ["*"]
    resources = ["*"]

    condition {
      test     = "StringNotEqualsIfExists"
      variable = "aws:RequestedRegion"

      values = [
        "ap-northeast-1",
        "eu-west-1",
        "sa-east-1",
        "us-east-2",
        "us-west-2",
      ]
    }
  }

  # Limit VPC
  statement {
    effect = "Allow"

    actions = [
      "ec2:Describe*",
      "ec2:Export*",
      "ec2:Get*",
      "ec2:Search*",
      "ec2:CreateTags",
      "ec2:DeleteTags",
      "ec2:CopySnapshot",
      "ec2:AuthorizeSecurityGroupIngress",
      "ec2:ModifyVolumeAttribute",
      "ec2:MonitorInstances",
      "ec2:CreateKeyPair",
      "ec2:CreateImage",
      "ec2:ResetInstanceAttribute",
      "ec2:CopyImage",
      "ec2:DeregisterInstanceEventNotificationAttributes",
      "ec2:ReplaceIamInstanceProfileAssociation",
      "ec2:UpdateSecurityGroupRuleDescriptionsIngress",
      "ec2:DeleteVolume",
      "ec2:StartInstances",
      "ec2:RevokeSecurityGroupEgress",
      "ec2:UnassignIpv6Addresses",
      "ec2:UnassignPrivateIpAddresses",
      "ec2:ImportImage",
      "ec2:DetachVolume",
      "ec2:ModifyVolume",
      "ec2:ResetImageAttribute",
      "ec2:UpdateSecurityGroupRuleDescriptionsEgress",
      "ec2:CancelExportTask",
      "ec2:ModifyAddressAttribute",
      "ec2:ImportKeyPair",
      "ec2:ResetNetworkInterfaceAttribute",
      "ec2:RegisterImage",
      "ec2:ModifyNetworkInterfaceAttribute",
      "ec2:DeleteNetworkInterface",
      "ec2:ModifyInstanceEventStartTime",
      "ec2:RunInstances",
      "ec2:ModifySecurityGroupRules",
      "ec2:StopInstances",
      "ec2:AssignPrivateIpAddresses",
      "ec2:ModifyHosts",
      "ec2:CreateVolume",
      "ec2:EnableVolumeIO",
      "ec2:RevokeSecurityGroupIngress",
      "ec2:CreateReplaceRootVolumeTask",
      "ec2:DisassociateIamInstanceProfile",
      "ec2:CreateSnapshots",
      "ec2:AssociateAddress",
      "ec2:DeleteKeyPair",
      "ec2:ModifyInstanceCapacityReservationAttributes",
      "ec2:ExportImage",
      "ec2:AttachVolume",
      "ec2:DisassociateAddress",
      "ec2:DeregisterImage",
      "ec2:ImportVolume",
      "ec2:DeleteSnapshot",
      "ec2:RequestSpotInstances",
      "ec2:RunScheduledInstances",
      "ec2:ModifyImageAttribute",
      "ec2:ReleaseHosts",
      "ec2:CreateSecurityGroup",
      "ec2:CreateSnapshot",
      "ec2:CreateStoreImageTask",
      "ec2:DeleteLaunchTemplateVersions",
      "ec2:ModifyInstanceAttribute",
      "ec2:ReleaseAddress",
      "ec2:RebootInstances",
      "ec2:CreateInstanceExportTask",
      "ec2:ModifyInstanceMetadataOptions",
      "ec2:AuthorizeSecurityGroupEgress",
      "ec2:DeleteLaunchTemplate",
      "ec2:ModifyInstancePlacement",
      "ec2:TerminateInstances",
      "ec2:CreateRestoreImageTask",
      "ec2:AssignIpv6Addresses",
      "ec2:DetachNetworkInterface",
      "ec2:ImportInstance",
      "ec2:ImportSnapshot",
      "ec2:AllocateAddress",
      "ec2:CreateLaunchTemplateVersion",
      "ec2:CreateLaunchTemplate",
      "ec2:ModifyInstanceCreditSpecification",
      "ec2:DeleteSecurityGroup",
      "ec2:AllocateHosts",
      "ec2:ModifyLaunchTemplate",
      "ec2:AttachNetworkInterface",
      "ec2:CancelImportTask",
    ]

    resources = ["*"]
  }

  statement {
    effect    = "Deny"
    actions   = ["ec2:*"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "developer" {
  name        = "Developer"
  description = "Developer access"
  policy      = data.aws_iam_policy_document.developer.json
}

data "aws_iam_policy_document" "developer_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

     principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::716876017850:role/Developer"]
    }
  }
}

resource "aws_iam_role" "developer" {
  name = "Developer"

  assume_role_policy = data.aws_iam_policy_document.developer_assume_role.json

  managed_policy_arns = [aws_iam_policy.developer.arn]

  tags = {
    Name        = "Developer"
    Environment = "sandbox"
    Owner       = "team-cloud@readyset.io"
    Service     = "IAM"
  }
}


