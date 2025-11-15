# S3 Zip File Processor for SMI

This script processes ZIP files stored in an Amazon S3 bucket. It extracts data from these ZIP files, converts text files to CSV format, and loads the data into a BigQuery dataset. The script ensures that only new files are processed based on their names.

## Table of Contents
- [Steps](#steps)
- [Requirements](#requirements)
- [Testing](#testing)

## Steps
- Lists files in the S3 Bucket.
- Fetches ZIP files from an Amazon S3 bucket.
- Executes a SQL query on BigQuery to extract relevant table names.
- Checks the names of the ZIP files against the extracted table names to identify new files.
- Downloads ZIP files, unzips them, converts `.txt` files to `.csv`, and loads the CSV files into BigQuery.
- All columns are set as `STRING` by default, as transformations will be done later using dbt.
- Automatically adds a timestamp and a `YYYYMM` column to the CSV files during conversion.

## Requirements
- Python 3.12 or higher.
- Navigate to `hexa-data-alteryx-workflows/src_docker/smi` and install the libraries listed in `requirements.txt`.
- Use `pyenv` or `conda` for environment management.
- Docker installed.
- AWS configured with sandbox credentials.

## Testing

### 1. Local Machine (Using pyenv/conda):
```bash
git clone https://github.com/hexaNZ/hexa-data-alteryx-workflows
cd src_docker
# Activate your pyenv or conda environment
cd smi
pip install -r requirements.txt
cd ..  # Go back to src_docker
python -m smi.main

### 
```bash
git clone https://github.com/hexaNZ/hexa-data-alteryx-workflows
cd src_docker
docker build -f smi-workflow.dockerfile -t smi-workflow .
docker run -v ~/.aws:/root/.aws smi-workflow