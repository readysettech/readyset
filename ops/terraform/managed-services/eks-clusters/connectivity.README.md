
1. Ensure SUBSTRATE_ROOT is set, using a command such as the following:

`export SUBSTRATE_ROOT=~/rsrepos/readyset/ops/substrate/root-modules/readyset/build/default/us-east-2/`

2. Generate a kubeconfig:

`aws eks update-kubeconfig --region us-east-2 --name rs-build-us-east-2 --profile build-admin`

MAKE SURE substrate hasn't exported its environment variables. It needs to go from your user -> build-admin role, etc.
