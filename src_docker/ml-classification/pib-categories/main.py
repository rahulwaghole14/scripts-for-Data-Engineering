"""Classify businesses based on user needs"""
import os
import sys

# import pandas as pd
import json
from sentence_transformers import SentenceTransformer, util

from google.cloud import bigquery

# from sentence_transformers import SentenceTransformer, util
from common.logging.logger import logger, log_start, log_end

# from common.validation.piifilter import get_entity_info, nlp
from common.bigquery.bigquery import create_bigquery_client, return_bq_sql_dict
from common.aws.aws_secret import get_secret
from common.validation.validators import validate_list_of_dicts
from pydantic import BaseModel
from typing import Optional


class BusinessDescription(BaseModel):
    Name_ID: Optional[int] = None
    Company_Name: Optional[str] = None
    Brand_id: Optional[str] = None
    Brand_name: Optional[str] = None
    business_description: Optional[str] = None
    PIB_CODE: Optional[str] = None
    PIB_CATEGORY: Optional[str] = None
    Classification: Optional[str] = None


#### need to change the table to right one we need for this project

# Initialize logger
logger = logger(
    "classification",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

model = SentenceTransformer("all-MiniLM-L6-v2")


def classify(businesses, categories):
    """Classify articles based on user needs"""

    user_need_embeddings = model.encode(categories)

    classifications = []

    for business in businesses:
        business_embedding = model.encode(business)
        cosine_scores = util.pytorch_cos_sim(
            business_embedding, user_need_embeddings
        )
        max_score_idx = cosine_scores.argmax()
        classification = categories[max_score_idx]
        classifications.append(classification)

    return classifications


def load_secrets():
    """load secrets from AWS Secrets Manager"""
    google_cred_secret = get_secret("GOOGLE_CLOUD_CRED_BASE64")
    return json.loads(google_cred_secret)["GOOGLE_CLOUD_CRED_BASE64"]


def main():
    """Main function"""
    try:
        log_start(logger)
        google_cred = load_secrets()
        client = create_bigquery_client(google_cred)

        # select_sql = """
        # SELECT Name_ID, Company_Name, Brand_Id, Brand_Name
        # FROM `hexa-data-report-etl-prod.adw_stage.naviga_companies`
        # WHERE is_person IS NULL
        # """
        # # return the company names list of dicts
        # company_names = return_bq_sql_dict(client, select_sql)
        # # print(company_names)

        # # run get_entity_info and determine if the company name is a person or not
        # # populate the is_person column with True or False based on the entity info
        # logger.info("Classifying company names")
        # for company in tqdm(company_names, desc="Classifying"):
        #     company["is_person"] = False
        #     entity_info = get_entity_info(company["Company_Name"], nlp)
        #     for entity in entity_info:
        #         if entity[1] == "PERSON":
        #             company["is_person"] = True
        #             break
        # # print(company_names)

        # # Write the updated data to a temporary table
        # # temp_table_id = (
        # #     "hexa-data-report-etl-prod.adw_stage.naviga_companies_temp"
        # # )
        # temp_table_ref = client.dataset("adw_stage").table(
        #     "naviga_companies_temp"
        # )

        # job_config = bigquery.LoadJobConfig(
        #     schema=[
        #         bigquery.SchemaField("Name_ID", "INT64"),
        #         bigquery.SchemaField("Company_Name", "STRING"),
        #         bigquery.SchemaField("Brand_Id", "STRING"),
        #         bigquery.SchemaField("Brand_Name", "STRING"),
        #         bigquery.SchemaField("is_person", "BOOL"),
        #     ],
        #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # )

        # load_job = client.load_table_from_json(
        #     company_names, temp_table_ref, job_config=job_config
        # )
        # load_job.result()  # Wait for the job to complete
        # logger.info("Loaded data into temporary table.")

        # # Merge the temporary table back into the original table
        # merge_sql = """
        # MERGE `hexa-data-report-etl-prod.adw_stage.naviga_companies` T
        # USING `hexa-data-report-etl-prod.adw_stage.naviga_companies_temp` S
        # ON T.Name_ID = S.Name_ID
        # AND (T.Company_Name = S.Company_Name OR (T.Company_Name IS NULL AND S.Company_Name IS NULL))
        # AND (T.Brand_Id = S.Brand_Id OR (T.Brand_Id IS NULL AND S.Brand_Id IS NULL))
        # AND (T.Brand_Name = S.Brand_Name OR (T.Brand_Name IS NULL AND S.Brand_Name IS NULL))
        # WHEN MATCHED THEN
        #   UPDATE SET T.is_person = S.is_person
        # """
        # query_job = client.query(merge_sql)
        # query_job.result()  # Wait for the job to complete
        # logger.info("Merged data back into the original table.")

        logger.info("starting classification for business categories")

        business_descriptions_sql = """
        SELECT Name_ID
        , Company_Name
        , Brand_id
        , Brand_name
        , CASE
            WHEN Company_Name != Brand_name THEN CONCAT(Company_Name, ' ', Brand_name, ' - ', JSON_EXTRACT_SCALAR(classification, '$.candidates[0].content.parts[0].text'))
            ELSE CONCAT(Company_Name, ' - ', JSON_EXTRACT_SCALAR(classification, '$.candidates[0].content.parts[0].text'))
        END AS business_description
        FROM `hexa-data-report-etl-prod.prod_dw_intermediate.int_business_descriptions`
        LIMIT 1
        """

        business_descriptions = return_bq_sql_dict(
            client, business_descriptions_sql
        )

        business_classifications_sql = """
        SELECT
        string_field_0 as PIB_CODE
        , string_field_1 AS PIB_CATEGORY
        -- , string_field_2 AS PIB_KEYWORDS
        , CONCAT(string_field_1, " ", REPLACE(REPLACE(REPLACE(REPLACE(string_field_2, "[", ""), "]", ""), "'", ""), ",", "")) AS CONCATENATED_CATEGORY_KEYWORDS
        FROM `hexa-data-report-etl-prod.adw_stage.naviga_pib_categories`
        """

        business_classifications = return_bq_sql_dict(
            client, business_classifications_sql
        )

        # Prepare data for classification
        business_texts = [
            item["business_description"] for item in business_descriptions
        ]
        category_texts = [
            item["CONCATENATED_CATEGORY_KEYWORDS"]
            for item in business_classifications
        ]

        # Classify business descriptions
        classifications = classify(business_texts, category_texts)

        # Add classifications to business descriptions
        for i, business in enumerate(business_descriptions):
            business["PIB_CODE"] = business_classifications[i]["PIB_CODE"]
            business["PIB_CATEGORY"] = business_classifications[i][
                "PIB_CATEGORY"
            ]
            business["Classification"] = classifications[i]

        # Prepare and validate data for BigQuery
        validated_data = validate_list_of_dicts(
            business_descriptions, BusinessDescription
        )

        logger.info("Data validated successfully. %s", validated_data)

        # # Write the updated data to a temporary table
        # temp_table_ref = client.dataset("adw_stage").table(
        #     "naviga_companies_temp"
        # )

        # job_config = bigquery.LoadJobConfig(
        #     schema=[
        #         bigquery.SchemaField("Name_ID", "INTEGER"),
        #         bigquery.SchemaField("Company_Name", "STRING"),
        #         bigquery.SchemaField("Brand_id", "STRING"),
        #         bigquery.SchemaField("Brand_name", "STRING"),
        #         bigquery.SchemaField("business_description", "STRING"),
        #         bigquery.SchemaField("PIB_CODE", "STRING"),
        #         bigquery.SchemaField("PIB_CATEGORY", "STRING"),
        #         bigquery.SchemaField("Classification", "STRING"),
        #     ],
        #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # )

        # load_job = client.load_table_from_json(
        #     validated_data, temp_table_ref, job_config=job_config
        # )
        # load_job.result()  # Wait for the job to complete
        # logger.info("Loaded data into temporary table.")

        # # Merge the temporary table back into the original table
        # merge_sql = """
        # MERGE `hexa-data-report-etl-prod.adw_stage.naviga_companies` T
        # USING `hexa-data-report-etl-prod.adw_stage.naviga_companies_temp` S
        #     ON T.Name_ID = S.Name_ID
        #     AND (T.Company_Name = S.Company_Name OR (T.Company_Name IS NULL AND S.Company_Name IS NULL))
        #     AND (T.Brand_Id = S.Brand_Id OR (T.Brand_Id IS NULL AND S.Brand_Id IS NULL))
        #     AND (T.Brand_Name = S.Brand_Name OR (T.Brand_Name IS NULL AND S.Brand_Name IS NULL))
        # WHEN MATCHED THEN
        #   UPDATE SET
        #     T.business_description = S.business_description,
        #     T.PIB_CODE = S.PIB_CODE,
        #     T.PIB_CATEGORY = S.PIB_CATEGORY
        # """
        # query_job = client.query(merge_sql)
        # query_job.result()  # Wait for the job to complete
        # logger.info("Merged data back into the original table.")

        log_end(logger)

    except Exception as e:  # pylint: disable=broad-except
        logger.error("Script failed with exception: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
