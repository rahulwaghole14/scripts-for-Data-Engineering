# src/classify.py

import os
import sys
import json
from datetime import datetime

from sentence_transformers import SentenceTransformer, util

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from common.logging.logger import logger, log_start, log_end
from common.bigquery.bigquery import create_bigquery_client
from common.aws.aws_secret import get_secret

# Initialize logger
logger = logger(
    "classification_drupal_article",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

SQL_QUERY = """
    SELECT
    articles.id as article_id,
    CONCAT(
        ANY_VALUE(articles.headline), ' ', 
        STRING_AGG(body.text, ' ')
    ) AS combined_article_text,
    ANY_VALUE(articles.publishedDate) AS published_date
    FROM
    `hexa-data-report-etl-prod.cdw_stage.drupal__articles` articles,
    UNNEST(body) AS body
    WHERE
    body.contentBodyType = 'text/html'
    AND DATETIME(articles.publishedDate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
    GROUP BY 1
"""

def create_classifications_table_if_not_exists(client):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS `hexa-data-report-etl-prod.cdw_stage.drupal_article_classifications`
    (
      article_id STRING,
      classification STRING,
      published_date TIMESTAMP,
      combined_article_text STRING(1000),  # Limit to 1000 characters
      timestamp TIMESTAMP,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    )
    PARTITION BY DATE(timestamp)
    CLUSTER BY article_id;
    """

    try:
        query_job = client.query(create_table_query)
        query_job.result()  # Wait for the query to complete
        logger.info("Classifications table created or already exists.")
    except Exception as e:
        logger.error(f"Error creating classifications table: {str(e)}")
        raise

def classify_articles(articles_data, user_needs):
    """Classify articles based on user needs"""
    model = SentenceTransformer("all-MiniLM-L6-v2")

    user_need_embeddings = model.encode(user_needs)

    classifications = []

    for article in articles_data:
        article_embedding = model.encode(article['combined_article_text'])
        cosine_scores = util.pytorch_cos_sim(
            article_embedding, user_need_embeddings
        )
        max_score_idx = cosine_scores.argmax()
        classification = user_needs[max_score_idx]
        classifications.append({
            'article_id': article['article_id'],
            'classification': classification,
            'published_date': article['published_date'],
            'combined_article_text': article['combined_article_text']
        })

    return classifications


def write_classifications_to_bigquery(client, classifications):
    """Write classification results to BigQuery"""
    table_id = "hexa-data-report-etl-prod.cdw_stage.drupal_article_classifications"
    
    def serialize_datetime(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    rows_to_insert = []
    for classification in classifications:
        row = {
            "article_id": str(classification['article_id']),
            "classification": classification['classification'],
            "published_date": serialize_datetime(classification['published_date']),
            "combined_article_text": classification['combined_article_text'][:1000],  # Truncate to first 1000 characters
            "timestamp": serialize_datetime(datetime.now())
        }
        rows_to_insert.append(row)

    # Split rows into batches of 1000
    batch_size = 1000
    for i in range(0, len(rows_to_insert), batch_size):
        batch = rows_to_insert[i:i+batch_size]
        
        errors = client.insert_rows_json(table_id, batch)
        if errors == []:
            logger.info(f"Successfully inserted batch of {len(batch)} rows into BigQuery.")
        else:
            logger.error(f"Encountered errors while inserting batch: {errors}")

    logger.info(f"Completed inserting all {len(rows_to_insert)} rows.")

def main():
    """Main function"""
    try:
        log_start(logger)

        # Get BigQuery client
        google_cred_secret = get_secret("GOOGLE_CLOUD_CRED_BASE64")
        google_cred = json.loads(google_cred_secret)["GOOGLE_CLOUD_CRED_BASE64"]
        client = create_bigquery_client(google_cred)

        # Run the query
        query_job = client.query(SQL_QUERY)

        # Process the results into a list of dictionaries
        results = [dict(row.items()) for row in query_job.result()]
        logger.info("Results: %s", results[0])

        # Define your user needs
        user_needs = [
            "update me, Provide me with the latest news and updates. Keep me informed about current events, trends, and important developments in various fields.",
            "Keep me engaged, Offer content that captivates my interest and holds my attention. This can include interactive features, intriguing stories, or thought-provoking discussions.",
            "educate me, Help me learn new information or skills. Provide educational content, tutorials, in-depth analysis, and explanations on a wide range of topics.",
            "give me perspective, Provide insights and viewpoints that help me understand different sides of an issue. Offer commentary, opinion pieces, and in-depth discussions that broaden my understanding.",
            "divert me, Entertain me and offer a distraction from daily life. This can include humorous content, light-hearted stories, games, and other forms of entertainment.",
            "inspire me, Motivate and uplift me. Share stories of success, creativity, and perseverance that encourage me to pursue my goals and dreams.",
            "help me, Offer practical advice and solutions to my problems. This can include how-to guides, troubleshooting tips, and other forms of assistance that make my life easier.",
            "connect me, Help me build relationships and engage with others. This can include social networking opportunities, community-building content, and platforms for discussion and interaction.",
        ]

        # Classify articles
        classifications = classify_articles(results, user_needs)
        logger.info(f"Classified {len(classifications)} articles")

        # Output classifications
        for classification in classifications:
            logger.info("Article ID: %s", classification['article_id'])
            logger.info("Classified as: %s", classification['classification'])

        try:
            create_classifications_table_if_not_exists(client)
            write_classifications_to_bigquery(client, classifications)
        except Exception as e:
            logger.error(f"Error in BigQuery operations: {str(e)}")
            sys.exit(1)

        log_end(logger)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Script failed with exception: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
    
    
    
    
# UPDATE `hexa-data-report-etl-prod.cdw_stage.drupal_article_classifications` AS classifications
# SET published_date = TIMESTAMP(articles.publishedDate)
# FROM `hexa-data-report-etl-prod.cdw_stage.drupal__articles` AS articles
# WHERE CAST(classifications.article_id as string) = cast(articles.id AS string);


### we can put this into dbt later


# CREATE OR REPLACE TABLE `your_project.your_dataset.drupal_article_monthly_page_views` AS
# WITH content_id AS (
#     SELECT DISTINCT CAST(article_id AS STRING) AS article_id  
#     FROM `hexa-data-report-etl-prod.cdw_stage.drupal_article_classifications`
# )
# , UniqueVisitors AS (
#     SELECT
#           POST_EVAR6 AS content_id
#         , COUNT(DISTINCT CONCAT(POST_VISID_LOW, POST_VISID_HIGH)) AS unique_visitors
#     FROM `hexa-data-report-etl-prod`.`prod_dw_intermediate`.`sat_adobe_analytics_masthead_replatform`
#     WHERE
#         DATE(LOAD_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) 
#         AND DATE(LOAD_DATE) < CURRENT_DATE('Pacific/Auckland')
#         AND LOWER(GEO_COUNTRY) = 'nzl'
#         AND POST_EVAR6 IN (SELECT article_id FROM content_id)
#     GROUP BY POST_EVAR6
# )
# , PageViews AS (
#     SELECT
#          POST_EVAR6 AS content_id 
#         , COUNT(*) AS daily_count
#     FROM `hexa-data-report-etl-prod`.`prod_dw_intermediate`.`sat_adobe_analytics_masthead_replatform`
#     WHERE
#         DATE(LOAD_DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) 
#         AND DATE(LOAD_DATE) < CURRENT_DATE('Pacific/Auckland')
#         AND LOWER(GEO_COUNTRY) = 'nzl'
#         AND POST_EVAR6 IN (SELECT article_id FROM content_id)
#     GROUP BY POST_EVAR6
# )
# SELECT
#       uv.content_id
#     , uv.unique_visitors
#     , pv.daily_count
# FROM UniqueVisitors uv
# INNER JOIN PageViews pv ON uv.content_id = pv.content_id
# ORDER BY uv.content_id;
