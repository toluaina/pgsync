### Development workflow

We don't have automated CICD pipelines for this repository. The deployment has to be done manually by following these steps. Note, that this is only one way to do this, you can use other approaches as well.

NOTE: This appraoch works for both prod and dev. You can specify the environment when running `aws configure sso`

1. Open Windows terminal with admin privileges, and open Powershell (just type powershell)
1. Navigate to the root of this repository
1. Build a new Docker image by running `docker build -t pgsync-ecr-repository .`
1. Upload the new Docker image to AWS ECR
    1. Run `aws configure sso`
    1. Run `aws ecr get-login-password --region eu-west-1 --profile AdministratorAccess-831959834821 | docker login --username AWS --password-stdin 831959834821.dkr.ecr.eu-west-1.amazonaws.com`
    1. Run `docker tag pgsync-ecr-repository:latest 831959834821.dkr.ecr.eu-west-1.amazonaws.com/pgsync-ecr-repository:latest`
    1. Run `docker push 831959834821.dkr.ecr.eu-west-1.amazonaws.com/pgsync-ecr-repository:latest`
    1. You can verify that the push was successful by viewing the images in AWS Console ECR
1. Restart pgsync task with AWS Console (ECS). No other changes needed, because the Cloudformation configuration fetches the latest image
1. Make mergechanges to main