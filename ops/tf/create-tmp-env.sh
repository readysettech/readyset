#!/bin/bash
set -eu -o pipefail
tf_dir=$(dirname $(readlink -f $0))

# If you want to deploy a specific version, set TF_VAR_readyset_commit to the git short hash.

# Initialize the Terraform environment. This ensures all the modules needed are installed.
terraform -chdir=$tf_dir/environments/tmp init
# Create a plan and save the results of the plan. -input=false will ensure that no input is needed.
terraform -chdir=$tf_dir/environments/tmp plan -input=false -out create_tmp_plan
# Show what the plan will do. This is also output by plan but this could eend up in another step.
terraform -chdir=$tf_dir/environments/tmp show create_tmp_plan
# Apply the create plan created in the previous step.
terraform -chdir=$tf_dir/environments/tmp apply -input=false create_tmp_plan
# Output a plan to destroy everything we made and save it.
terraform -chdir=$tf_dir/environments/tmp plan -destroy -input=false -out destroy_tmp_plan

# To destroy the created environment, run:
# terraform -chdir=$tf_dir/environmnents/tmp apply -input=false destroy_tmp_plan
