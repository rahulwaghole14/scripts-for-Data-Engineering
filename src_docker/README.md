# Project Name
hexa Data Alteryx Workflows

## Description
Contains python scripts and Alteryx workflows to process data from various data sources and load them into BigQuery or downstream systems.
Confluence documentation:
- [main page](https://hexanz.atlassian.net/wiki/spaces/CD/overview?homepageId=2435809341)
- [data products](https://hexanz.atlassian.net/wiki/spaces/CD/database/2970058835)

Many of the scripts are designed to run in a Docker container. The Dockerfile is located in the `src_docker` directory for each script. We use the `Dockerfile` to build the Docker image and run the container. The `README.md` file in the `src_docker` directory contains instructions on how to build and run the Docker container see below.

## Prerequisites
- Docker installed on your machine
- AWS CLI
- AWS Credentials (Access Key ID and Secret Access Key)
- AWS setup details: https://docs.google.com/document/d/19VXPYGMfqtqNKbH1bUzqFr2Ic2irGuCKIP4EWfmgxM4/edit

## Testing code locally using python
1. Clone the repository: `git clone https://github.com/hexaNZ/hexa-data-alteryx-workflows.git`
2. Navigate to the project directory: `cd src_docker`
3. Setup pyenv environment
4. Run pip install brightcove-videos-to-bigquery/requirements.txt
5. Run python -m brightcove-videos-to-bigquery.main


## Build & Test Docker Image locally
1. Clone the repository: `git clone https://github.com/hexaNZ/hexa-data-alteryx-workflows.git`
2. Navigate to the project directory: `cd src_docker`
3. Build the Docker image locally: `docker build -f brightcove-videos-to-bigquery/Dockerfile -t brightcove-test .`
4. Run the Docker container in detached mode: `docker run -d brightcove-test`
    5. Optionally Run the Docker container with logging: `docker run -it brightcove-test`
    6. Optionally Run the Docker container with logging and access aws resources (secrets) locally: `docker run -it -v ~/.aws:/root/.aws brightcove-test`


## Configure AWS Credentials
1. Create a file named `credentials` in the `~/.aws/` directory.
2. Open the `credentials` file and add the following lines:
    ```
    [default]
    aws_access_key_id = [your access key id]
    aws_secret_access_key = [your secret access key]
    ```
