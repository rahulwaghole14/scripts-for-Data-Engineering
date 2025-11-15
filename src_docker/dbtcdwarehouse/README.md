# CDW dbt Management Project

This is a dbt project currently used in production for Bigquery Sentiment mart. This project is currently dbt project plus a 'scripts' folder containing legacy database scripts from CDW (Stored procedures) that can be referenced for logic migration. The main use case at the moment is to maintain CDW marts in Bigquery but it could be extended to RDW or Advertising Data Warehouses to build a central Enterprise Data Warehouse.

---
## example docker usage:
```dockerfile
# Use an official Python runtime as a parent image
FROM python:3.12

# Set the working directory in the container to /app
WORKDIR /usr/src/app

# Display the current working directory
RUN pwd

# Copy the contents of the brightcove-workflow directory into /usr/src/app
#COPY . .
COPY dbtcdwarehouse dbtcdwarehouse
COPY common common

#COPY hexa-data-alteryx-workflows/src_docker/common/ common
# Optionally, copy the common directory into /usr/src/app
# COPY common /usr/src/app/common

# Install any needed packages specified in requirements.txt
WORKDIR /usr/src/app/dbtcdwarehouse
RUN pip install --no-cache-dir -r requirements.txt
RUN dbt deps

#RUN pip install --no-cache-dir -r /usr/src/app/brightcove-workflow/requirements.txt
WORKDIR /usr/src/app

# Make port 80 available to the world outside this container
EXPOSE 80

# WORKDIR /usr/src/app/dbtcdwarehouse/scripts
# Specify the Python command to run the application
CMD ["python", "-m", "dbtcdwarehouse.scripts.test"]
```


## Table of Contents

1. [dbt Resources](#dbt-resources)
2. [Missing Files](#missing-files)
3. [Contributing](#contributing)
4. [Local Environment Setup](#local-environment-setup)
5. [DBT Project Structure](#dbt-project-structure)
6. [Archive](#archive)

---

## dbt Resources

- [Learn more about dbt](https://docs.getdbt.com/docs/introduction)
- [dbt Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- [Join the dbt community chat](https://community.getdbt.com/) on Slack
- [dbt Events](https://events.getdbt.com)
- [dbt Blog](https://blog.getdbt.com/)

---

## update access for reader accounts:
make sure to add tables and views to `grant_access.sql` file and run the script to update access for reader accounts in production:
make sure to add new accounts to `grant_access.sql` file and run the script to update access for reader accounts in production:
make sure to add policy tags to pii tables and that reader accounts have masked reader role in gcloud IAM

  ```bash
  dbt run-operation grant_access
  ```

## Missing Files

The `profiles.yml` file in the root of the project is currently excluded, as it contains database credentials. Work is underway to include the file using parameterization. Ask another developer to show you how to set this up.

---

## Contributing

Refer to the [contributing guide](contributing.md) for instructions on how to contribute to this project.

---

## Local Environment Setup

```bash
$ pyenv virtualenv 3.9 cdwarehouse
$ pyenv activate cdwarehouse
$ python3 -m pip install -r requirements.txt
```

# DBT Project Structure

## Environment Workflows

### Environments

- **Local Environments**:
  - `dpowell_dw_staging`
  - `rbhaskhar_dw_staging`

- **Dev**: `dev_dw_staging`
  - **Note**: Data in the dev environment is not complete.

- **Production**: `prod_dw_staging`

## dbt testing
1. check `profiles.yml` dataset to see where runs will materials, do not use `prod`
2. clean up workspace (local) `dbt clean`
3. install deps `dbt deps`
4. `dbt run` etc...

## dbt production behavior & usage
1. create the stage/vault/marts all together `dbt run --select +tag:adobe`
2. full refresh mart only `dbt run --select tag:adobe --full-refresh` notice removed +/plus sign
3. full refresh vault only `dbt run --select +tag:vault_adobe --full-refresh` notice removed +/plus sign
4. NOTE do not run a combination of `+tag:adobe` and `--full-refresh` it will cause data issues in vault

## using elementary dbt commands
this was added to the `dbt_project.yml` file
```yaml
models:
  elementary:
    +schema: "elementary"
```
`pip install elementary-data[bigquery]`
`dbt deps` - install dependencies for elementary dbt project
`dbt run --select elementary` - run elementary dbt project (set up base tables)

### elementary slack alerts
https://docs.elementary-data.com/oss/guides/alerts/send-slack-alerts

make sure to add `--env prod` in production

To alert on source freshness, you will need to run `edr run-operation upload-source-freshness --project-dir ./` right after each execution of `dbt source freshness`. This operation will upload the results to a table, and the execution of `edr monitor` will send the actual alert.

`edr monitor --slack-token <your_slack_token> --slack-channel-name <slack_channel_to_post_at> --group-by [table | alert] --env prod`

## Data Marts Legacy Names from CDW (SqlServer)

- `smart`
- `imart`

## Naming Convention in BigQuery

1. **Staging**: `[env_prefix]_dw_staging`
    - **Purpose**: Basic models for renaming fields, adjusting data types.

2. **Intermediate**: `[env_prefix]_dw_intermediate`
    - **Purpose**: Raw vault and business vault models.

3. **Marts**: `[env_prefix]_dw_mart_[name]`
    - **Examples**:
      - `dpowell_dw_mart_sentiment`, `prod_dw_mart_sentiment`
      - `dpowell_dw_mart_identity`, `prod_dw_mart_identity`

# Datafold datadiff usage

## Installation:

  ```bash
  pip install -r requirements.txt
  ```

added configuration to dbt_project.yml (already done)

  ```yaml
  vars:
  data_diff:
    prod_database: hexa-data-report-etl-prod
    prod_schema: prod
    prod_custom_schema: prod_<custom_schema>
  ```

## Authentication

Only dbt projects that use the OAuth via gcloud connection method are currently supported.
For example, change profiles.yml and run the command to authenticate:

profiles.yml:

  ```yml
  cdw:
    target: dev
    outputs:
      # oauth
      # howto authenticate: run in terminal: `gcloud auth application-default login`
      dev:
        type: bigquery
        location: australia-southeast1
        method: oauth
        project: hexa-data-report-etl-dev
        dataset: dpowell
        threads: 4
  ```

authenticate:

  ```bash
  gcloud auth application-default login
  ```

before running a `data-diff` command.

## Run with --dbt
Run your dbt model with data-diff --dbt to see the impact that your model change had on the data.

### as one command
Note: models must have primary keys defined in documentation i.e. uniqueness tests before they will work with data-diff

    ```bash
    dbt run --select <MODEL> && data-diff --dbt
    dbt run --select <MODEL> && data-diff --dbt -k <KEY> --json
    ```

### or as separate commands

    ```bash
    dbt run --select <MODEL>
    data-diff --dbt
    ```

Optional configurations and flags
Running data-diff on specific dbt models
Out of the box, data-diff --dbt will diff all models that were built in your last dbt run.

Beginning with data-diff version 0.7.5, you can add a --select flag to override the default behavior and specify which models you want to diff.

data-diff --dbt --select <models>

Handling very large dbt models
data-diff will reach performance limitations on large dbt models. One strategy to reduce run time in this scenario is to add a filter, which is essentially a where clause that is configured in that model's yml. This defines which rows will be diffed.

Another option is to limit the number of columns via the Include / Exclude Columns configuration.

# Archive
```sql
GRANT `ROLE_LIST`
ON SCHEMA RESOURCE_NAME
TO "USER_LIST"
;
GRANT `roles/bigquery.dataViewer`
ON SCHEMA `myProject`.myDataset
TO "user:raha@example-pet-store.com", "user:sasha@example-pet-store.com"
```
```sql
GRANT `ROLE_LIST`
ON RESOURCE_TYPE RESOURCE_NAME
TO "USER_LIST"
;
GRANT `roles/bigquery.dataViewer`
ON TABLE `myProject`.myDataset.myTable
TO "user:raha@example-pet-store.com", "user:sasha@example-pet-store.com"
```
