from diagrams import Diagram, Cluster
from diagrams.aws.compute import EC2
from diagrams.aws.database import RDS
from diagrams.aws.security import SecretsManager
from diagrams.aws.network import VPC
from diagrams.aws.management import Cloudwatch
from diagrams.onprem.vcs import Github
from diagrams.programming.language import Python

with Diagram("AWS Airbyte Server Architecture", show=True):

    with Cluster("VPC"):
        ec2 = EC2("Airbyte Server [tag: airbyte]")
        rds = RDS("Postgres Database")
        secret_manager = SecretsManager("Secrets Manager")  # Changed here
        cloudwatch = Cloudwatch("Logging with CloudWatch")
        vpc = VPC("VPC")

        ec2 - secret_manager
        ec2 - rds
        ec2 - cloudwatch
        vpc - ec2

    with Cluster("GitHub Repo"):
        github = Github("GitHub Repo for CaC Code")

    with Cluster("CLI Tool"):
        octavia_cli = Python("Octavia CLI")

    github - octavia_cli

    ec2 - github
    octavia_cli - ec2
