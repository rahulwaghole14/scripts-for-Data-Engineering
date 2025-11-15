""" update sponsored content segment """
import logging
import re
import os
import json
import pandas
from dotenv import load_dotenv
import base64
from common import update_segment, get_token
from gsheets_data import get_gsheet_dataframe, keep_last_n_days
from oauth2client.service_account import ServiceAccountCredentials

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("sponsored_content.log"),
        logging.StreamHandler(),
    ],
)

RSID = "fairfaxnz-hexaoverall-production"
OWNER = 200100424  # service_account
SEGMENT_ID = "s200000657_5db8a82471ac0108a6900555"
NAME = "sponsored_content_individual_test"
DESCRIPTION = "updated"


def get_content_id(article_link):
    """find content ids in the gsheet"""
    id1 = "".join(re.findall(r"\d+", str(re.findall(r"/1.*/", article_link))))
    id3 = "".join(re.findall(r"\d+", str(re.findall(r"/3.*/", article_link))))
    return id1 + id3


if __name__ == "__main__":

    try:
        logging.info("STARTED - Sponsored Content")
        load_dotenv()
        try:
            encoded_adobecred_sponcon_env = os.getenv("adobecred_sponcon_env")
            encoded_adobepriv_env = os.getenv("adobepriv_env")
            if (
                encoded_adobecred_sponcon_env is None
                or encoded_adobepriv_env is None
            ):
                logging.error("One or both environment variables are not set.")

            else:
                adobecred_json = base64.b64decode(
                    encoded_adobecred_sponcon_env
                ).decode("utf-8")
                private_key = base64.b64decode(encoded_adobepriv_env).decode(
                    "utf-8"
                )

                adobecred = json.loads(adobecred_json)

                # Now you have `adobecred` as a dictionary and `private_key` as a string
                token = get_token(adobecred, private_key)
                ACCESS_TOKEN = token["access_token"]
                logging.info("Access token obtained successfully.")

        except json.JSONDecodeError as e:
            logging.info("JSON decode error: %s", e)
        except Exception as e:
            logging.info("An error occurred: %s", e)

        # api secrets
        ADOBE_CRED_SPON_CON = adobecred
        BASE_URL = os.environ.get("ADOBE_API_BASE_URL")
        COMPANY_ID = os.environ.get("ADOBE_API_COMPANY_ID")
        X_API_KEY = adobecred["X_API_KEY"]
        BASE_URL = BASE_URL + COMPANY_ID + "/"

        REQUEST_HEADERS = {
            "Accept": "application/json",
            "Authorization": "Bearer " + ACCESS_TOKEN,
            "Content-Type": "application/json",
            "x-api-key": X_API_KEY,
            "x-proxy-global-company-id": COMPANY_ID,
        }

        try:

            encoded_GOOGLE_CLOUD_CRED_SPON_CON_env = os.getenv(
                "GOOGLE_CLOUD_CRED_SPON_CON_env"
            )
            if encoded_GOOGLE_CLOUD_CRED_SPON_CON_env is None:
                logging.error(
                    "GOOGLE_CLOUD_CRED_SPON_CON_env env variable is not set."
                )
            else:
                # decode the base64 encoded string
                google_creds = base64.b64decode(
                    encoded_GOOGLE_CLOUD_CRED_SPON_CON_env
                ).decode("utf-8")
                google_creds = json.loads(google_creds)  # convert to json

            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
            GOOGLE_CLOUD_CRED_SPON_CON = (
                ServiceAccountCredentials.from_json_keyfile_dict(
                    google_creds, scope
                )
            )
            logging.info(
                "set ServiceAccountCredentials into GOOGLE_CLOUD_CRED_SPON_CON"
            )

        except json.JSONDecodeError as e:
            logging.info("JSON decode error: %s", e)
        except Exception as e:
            logging.info("An error occurred: %s", e)

        logging.info("Retrieve Sponsored Content data from Google Sheets")
        df = get_gsheet_dataframe(
            spreadsheet_name="Sponsored content campaign URLs",
            head=1,
            credentials=GOOGLE_CLOUD_CRED_SPON_CON,
        )
        logging.info("Rows read from spreadsheet = %s", str(df.shape[0]))

        #     # Clean downloaded Google Sheets
        df = df[["START DATE", "ARTICLE LINK"]]
        df["START DATE"] = pandas.to_datetime(df["START DATE"])
        df["content_id"] = df["ARTICLE LINK"].apply(get_content_id)
        df = df[pandas.to_numeric(df["content_id"], errors="coerce").notnull()]
        logging.info(
            "Rows after filtering valid content_id = %s", str(df.shape[0])
        )

        # # Define the specific ID you're looking for
        # specific_id = '133291750'

        # # Check if the specific ID is in the 'content_id'
        # # column of the DataFrame
        # is_id_present = specific_id in df['content_id'].values

        # # Print result
        # logging.info(f"Is the specific ID ({specific_id})
        # present in the DataFrame? {is_id_present}")

        # df = keep_last_n_days(df,150)
        # logging.info('Rows after filtering last 150 days = %s', str(df.shape[0]))

        segment_list = df["content_id"].values.tolist()
        segment_list = [str(x) for x in segment_list]
        # logging.info(segment_list)

        definition = {
            "container": {
                "func": "container",
                "pred": {
                    "val": {"func": "attr", "name": "variables/prop11"},
                    "func": "contains-any-of",
                    "description": "Content ID",
                    "list": segment_list,
                },
                "context": "hits",
            },
            "func": "segment",
            "version": [1, 0, 0],
        }

        logging.info("Update Adobe Segment")
        u = update_segment(
            RSID,
            OWNER,
            SEGMENT_ID,
            NAME,
            DESCRIPTION,
            BASE_URL,
            REQUEST_HEADERS,
            definition,
        )
        logging.debug(u.json())

        logging.info("FINISHED - Sponsored Content")

    except Exception as e:
        logging.info("Error: %s", e)
