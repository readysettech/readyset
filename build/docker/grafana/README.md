# readyset-grafana

This is the Dockerfile and build context for the `readyset-grafana` image, which is used by the Docker Compose quickstart in `readyset/public-docs/docs/assets/compose[.mysql|.postgres].yml`

Public ECR Gallery:
https://gallery.ecr.aws/readyset/readyset-grafana

Private ECR Repo:
https://us-east-2.console.aws.amazon.com/ecr/repositories/public/888984949675/readyset-grafana?region=us-east-2

## Deployment Steps

1. Obtain the terminal credentials for the `deploy` (888984949675) AWS account (see https://readysettech.atlassian.net/wiki/spaces/ENG/pages/27197446/AWS+CLI+Access):
    ```
    export AWS_PROFILE=deploy-AdministratorAccess
    aws sso login
    ```
1. Log into the ECR:
    ```
    aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 888984949675.dkr.ecr.us-east-2.amazonaws.com
    ```
1. Build, tag, and push the individual images, then generate the multi-arch manifest:
    ```
    docker build --platform=linux/arm64 -t public.ecr.aws/readyset/readyset-grafana:latest_linux_arm64 .

    docker build --platform=linux/amd64 -t public.ecr.aws/readyset/readyset-grafana:latest_linux_amd64 .

    docker push public.ecr.aws/readyset/readyset-grafana:latest_linux_arm64

    docker push public.ecr.aws/readyset/readyset-grafana:latest_linux_amd64
    ```
1. Delete the existing manifest from the ECR repo via the AWS console:
    > Pushing a new manifest with the same tag (`latest`) didn't work for me, ECR kept the old one.
    > I also couldn't use the AWS CLI to do it, something about it being confused between the private `888984949675` repo and `public.ecr.aws`.
    ```
    aws ecr batch-delete-image --repository-name public.ecr.aws/readyset/readyset-grafana --image-ids imageTag=latest

    An error occurred (RepositoryNotFoundException) when calling the BatchDeleteImage operation: The repository with name 'public.ecr.aws/readyset/readyset-grafana' does not exist in the registry with id '888984949675'
    ```
1. Generate and push the multi-arch manifest:
    ```
    docker manifest create public.ecr.aws/readyset/readyset-grafana:latest public.ecr.aws/readyset/readyset-grafana:latest_linux_arm64 public.ecr.aws/readyset/readyset-grafana:latest_linux_amd64

    docker manifest push --purge public.ecr.aws/readyset/readyset-grafana:latest
    ```