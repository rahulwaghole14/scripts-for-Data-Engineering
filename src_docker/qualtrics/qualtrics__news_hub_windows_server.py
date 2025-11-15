import sys
import os
import json
from datetime import datetime, timedelta, timezone
import pytz
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from qualtrics.qualtrics_data_process_windows_server import qualtrics_run

if __name__ == "__main__":
    load_dotenv()

    try:
        survey_id = os.getenv("QUALTRICS_NEWS_HUB_SURVEY_ID", "")
        survey_name = "NEWS_HUB_SURVEY"

        if not survey_id:
            raise ValueError("Survey ID not found in environment variables")

        # Set the start and end dates in UTC
        end_date_utc = datetime.now(timezone.utc)
        start_date_utc = end_date_utc - timedelta(days=3)

        # Convert UTC to NZT
        nzt_zone = pytz.timezone("Pacific/Auckland")

        end_date_nzt = end_date_utc.astimezone(nzt_zone)
        start_date_nzt = start_date_utc.astimezone(nzt_zone)

        # Ensure the date strings are in the correct format
        end_date_str = end_date_nzt.strftime("%Y-%m-%dT%H:%M:%SZ")
        start_date_str = start_date_nzt.strftime("%Y-%m-%dT%H:%M:%SZ")

        qualtrics_run(survey_id, survey_name, start_date_str, end_date_str)

    except Exception as e:
        print(f"Error: {e}")
