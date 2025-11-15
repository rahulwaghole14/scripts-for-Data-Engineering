from diagrams import Diagram, Cluster
from diagrams.aws.compute import EKS
from diagrams.aws.database import RDS
from diagrams.aws.security import SecretsManager
from diagrams.aws.management import Cloudwatch
from diagrams.onprem.vcs import Github
from diagrams.azure.devops import Pipelines
from diagrams.aws.compute import EC2

# from diagrams.onprem.compute import Docker

with Diagram(
    "Connect to Neighbourly RDS DB instance from Local and EKS Cluster Architecture",
    show=True,
):

    with Cluster("Neighbourly_Production AWS Account"):
        neighbourly_rds = RDS("Neighbourly RDS Reader")

    with Cluster("hexa-Sandbox AWS Account"):
        with Cluster("VPC"):
            eks_cluster = EKS("EKS Cluster Docker Images")
            our_secret_manager = SecretsManager("Secrets Manager")
            our_cloudwatch = Cloudwatch("CloudWatch Logging")

            eks_cluster - neighbourly_rds

    with Cluster("Prod AWS Account?"):
        with Cluster("VPC"):
            eks_cluster_prod = EKS("EKS Cluster Docker Images")
            our_secret_manager = SecretsManager("Secrets Manager")
            our_cloudwatch = Cloudwatch("CloudWatch Logging")

            eks_cluster_prod - neighbourly_rds

    with Cluster("Local Setup"):
        local_docker = EC2("Local Docker Images")
        local_docker - neighbourly_rds

    with Cluster("GitHub Repo"):
        github = Github("GitHub Repo for Docker Code")

    with Cluster("Azure Pipelines"):
        azure_pipelines = Pipelines("Azure Pipelines")

    github - azure_pipelines
    azure_pipelines - eks_cluster
    azure_pipelines - eks_cluster_prod
    local_docker - github
