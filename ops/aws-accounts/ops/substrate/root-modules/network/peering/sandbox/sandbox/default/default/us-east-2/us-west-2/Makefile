# managed by Substrate; do not edit by hand

AUTO_APPROVE=
#AUTO_APPROVE=-auto-approve

all:

apply:
	terraform apply $(AUTO_APPROVE)

destroy:
	terraform destroy $(AUTO_APPROVE)

init:
	terraform init -reconfigure

plan:
	terraform plan

.PHONY: all apply destroy init plan
