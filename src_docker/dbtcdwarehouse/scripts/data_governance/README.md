# Project Setup Instructions

## Terraform Installation and Setup

# google cloud setup
1. ```bash
    gcloud auth application-default login
    ```

1. **Install Terraform**:
    Ensure that Terraform is installed on your local machine. For installation instructions, refer to the [Terraform official documentation](https://www.terraform.io/downloads.html).

2. **Initialize Terraform**:
    ```bash
    terraform init
    ```
3. **Plan Terraform Deployment**:
    ```bash
    terraform plan
    ```

4. **Apply Terraform Configuration**:
    ```bash
    terraform apply
    ```
## run specific env

$ terraform init -var-file="backend-prod.hcl"

## Google cloud configuration check

1. **View taxonomies in gcloud cloud shell**:
    this can only be done after the terraform has been applied
    ```bash
    gcloud data-catalog taxonomies list --location='australia-southeast1'
    ```
2. **View policy tags in gcloud console**:
    this can be done by navigating to the bigquery console and then clicking on the policy tags tab

it doesn't matter how this is initially set up, because we will be able to iterate on this. however here would be the initial plan for role based access control in bigquery to certain fields:


| read (select)           | write (append data to table) | execute (delete table) |
|-------------------------|------------------------------|------------------------|
| - admin                 | - admin                      | - admin                |
| - data engineer (full)  | - data engineer (full)       | - data engineer (full) |
| - braze (crm) (full)    | - braze (none)               | - braze (none)         |
| - data analyst (user or | - data analyst (none)        | - data analyst (none)  |
|  powerbi)               |                              |                        |
| (restricted to standard |                              |                        |
| columns with PII columns|                              |                        |
| masked)                 |                              |                        |
|                         |                              |                        |
|-------------------------|------------------------------|------------------------|

the only initial requirement is to have one taxonomy called 'data_governance' with the following policy tags:
'pii_masked'

example pii fields to be masked
- email
- phone
- address
- name
- ip address

firstly any service account or user that needs to access the data in the table will need to have the following roles, if they are not already assigned they will get the error like this in bigquery:

```Access Denied: BigQuery BigQuery: User has neither fine-grained reader nor masked get permission to get data protected by policy tag "data_governance : pii_masked" on column hexa-data-report-etl-dev.dpowell_dw_staging.policy_tag_table.field.
```
Fine-grained reader role is required to read the data in the table, and masked get permission is required to read the masked data in the table.

i.e. all internal users will need the fine-grained reader role, and all external users will need the masked rol applied to their service account(s).

please read these to understand the concept of policy tags and data masking in BigQuery:
https://cloud.google.com/bigquery/docs/column-level-security-intro
https://cloud.google.com/bigquery/docs/column-data-masking-intro

we needed to enable two apis in google to get this working:
Google Cloud Data Catalog API (service name: datacatalog.googleapis.com)
and
bigquery data policy api (service name: bigquerydatapolicy.googleapis.com)


policytags and taxonomies can be found in bigquery > policytags in the console

https://docs.getdbt.com/reference/resource-configs/bigquery-configs
https://hexanz.atlassian.net/browse/CD-1089

BigQuery enables column-level security by setting policy tags on specific columns.

dbt enables this feature as a column resource property, policy_tags (not a node config).

# example yml config
```yaml
version: 2

models:
- name: policy_tag_table
  columns:
    - name: field
      policy_tags:
        - 'projects/<gcp-project>/locations/<location>/taxonomies/<taxonomy>/policyTags/<tag>'
```

Please note that in order for policy tags to take effect, column-level persist_docs must be enabled for the model, seed, or snapshot. Consider using variables to manage taxonomies and make sure to add the required security roles to your BigQuery service account key.

https://docs.getdbt.com/reference/resource-configs/persist_docs
