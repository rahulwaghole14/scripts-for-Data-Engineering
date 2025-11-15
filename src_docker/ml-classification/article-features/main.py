"""
main
"""
# src/classify.py

import os
import sys
import json

from sentence_transformers import SentenceTransformer, util
from common.logging.logger import logger, log_start, log_end
from common.bigquery.bigquery import create_bigquery_client
from common.aws.aws_secret import get_secret

# Initialize logger
logger = logger(
    "classification",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)


def classify_articles(articles, user_needs):
    """Classify articles based on user needs"""
    model = SentenceTransformer("all-MiniLM-L6-v2")

    user_need_embeddings = model.encode(user_needs)

    classifications = []

    for article in articles:
        article_embedding = model.encode(article)
        cosine_scores = util.pytorch_cos_sim(
            article_embedding, user_need_embeddings
        )
        max_score_idx = cosine_scores.argmax()
        classification = user_needs[max_score_idx]
        classifications.append(classification)

    return classifications


def main():
    """Main function"""
    try:
        log_start(logger)

        # Load your articles
        articles = [
            "This is the first article text. update me",
            "This is the second article text. divert me",
            "This is the third article text. inspire me",
            """New footage shows behind stage view of the aftermath of
            assassination attempt
            on Trump New footage shows the scramble to protect Donald Trump
            moments after he was shot at.
            The footage shows a new angle from behind the stage, moments after
            the assassination attempt in Butler,
            Pennsylvania. Trump  is seen surrounded by security guards as they
            try to escort him off the stage
            safely. Their efforts slowed down briefly as Trump raised his
            fist.""",
            """Annual inflation dropped to 3.3% in the three months to the
            end of June, down
            from 4% the previous quarter, Stats NZ has reported. Stats NZ
            senior manager Nicola Growden said
            inflation had now fallen to levels seen about three years ago,
            though she noted it was still above the top of
            the Reserve Bank’s 1% to 3% target band. Westpac chief economist
            Kelly Eckhold said the figures probably
            made an earlier interest-rate cut from the Reserve Bank marginally
            more likely, though not
            unequivocally so. Kiwibank, which has been forecasting a rate-cut
            in November, said prospects for “an
            even earlier cut” were rising.""",
        ]

        # Define your user needs
        # these are from bbc user needs framework
        user_needs = [
            """update me, Provide me with the latest news and updates. Keep me
            informed about current events, trends, and important developments
            in various fields.""",
            """Keep me engaged, Offer content that captivates my interest and holds
            my attention. This can include interactive features, intriguing stories,
            or thought-provoking discussions.""",
            """educate me, Help me learn new information or skills. Provide
            educational content, tutorials, in-depth analysis, and
            explanations on a wide range of topics.""",
            """give me perspective, Provide insights and viewpoints that help me
            understand different sides of an issue. Offer commentary, opinion
            pieces, and in-depth discussions that broaden my understanding.""",
            """divert me, Entertain me and offer a distraction from daily life.
            This can include humorous content, light-hearted stories, games, and
            other forms of entertainment.""",
            """inspire me, Motivate and uplift me. Share stories of success,
            creativity, and perseverance that encourage me to pursue my goals and
            dreams.""",
            """help me, Offer practical advice and solutions to my problems. This
            can include how-to guides, troubleshooting tips, and other forms of
            assistance that make my life easier.""",
            """connect me, Help me build relationships and engage with others.
            This can include social networking opportunities, community-building
            content, and platforms for discussion and interaction.""",
        ]

        # cdw_stage.drupal__articles
        # cdw_stage.drupal__articles_features

        # articles_id, bbc_user_needs, hexa_user_needs, breaking_news (1,0), article_embedding
        # 123, connect me
        # 124, help me
        # 125, inspire me
        # 126, connect me

        # adobe data pinned onto articles data
        # dimensions: day
        # metrics: page_views, unique_page_views
        # metrics (nice to have): time_on_page, entrances, exits, logins

        # analytics data
        # hypothesis: if we actively target and investigate user needs,
        # and understand what the users want to read,
        # we can direct the editorial team to produce more
        # of the content the users want and we can increase pageviews and ROI on advertising

        # idea for analysis: is there a significant difference
        # in pageviews or any other metric between
        # articles that are classified as "update me" vs "inspire me" vs "divert me"
        # based on amount of articles published overtime

        # Classify articles
        classifications = classify_articles(articles, user_needs)

        # Output classifications
        for article, classification in zip(articles, classifications):
            logger.info("Article: %s", article)
            logger.info("Classified as: %s", classification)

        log_end(logger)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Script failed with exception: %s", e)
        sys.exit(1)


# SQL_QUERY = """
#   SELECT
#     articles.id as article_id
#     , CONCAT(
#     ANY_VALUE(articles.headline), ' ', STRING_AGG(
#     REGEXP_REPLACE(
#       REGEXP_REPLACE(body.text, r'<[^>]+>', ''),  -- Remove HTML tags
#       r'http\S+|www\.\S+', ''  -- Remove URLs
#     ), ' '
#     )) AS combined_article_text
#   FROM
#     `hexa-data-report-etl-prod.cdw_stage.drupal__articles` articles,
#     UNNEST(body) AS body
#   WHERE
#     body.contentBodyType = 'text/html'
#   GROUP BY 1
#   LIMIT 10
#   """


SQL_QUERY = """
  SELECT
    articles.id as article_id
    , CONCAT(
    ANY_VALUE(articles.headline), ' ', STRING_AGG(body.text, ' ')) AS combined_article_text
  FROM
    `hexa-data-report-etl-prod.cdw_stage.drupal__articles` articles,
    UNNEST(body) AS body
  WHERE
    body.contentBodyType = 'text/html'
  GROUP BY 1
  LIMIT 10
  """

if __name__ == "__main__":
    google_cred_secret = get_secret("GOOGLE_CLOUD_CRED_BASE64")
    google_cred = json.loads(google_cred_secret)["GOOGLE_CLOUD_CRED_BASE64"]
    client = create_bigquery_client(google_cred)

    # Run the query
    query_job = client.query(SQL_QUERY)

    # Process the results into a list of dictionaries
    results = [dict(row.items()) for row in query_job.result()]
    logger.info("Results: %s", results[0])
    # main()
