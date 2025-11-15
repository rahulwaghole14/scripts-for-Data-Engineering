import requests
import pandas as pd
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)-5.5s]  %(message)s"
)

print(pd.__version__)

# API Setup
base_url = "https://dashboard.presspatron.com"
headers = {
    "Authorization": "Token 20039f24-0dd8-4df8-8b02-92b1ef601b71"
}  # Replace with your actual token


def download_dataframe_no_pagelimit(resource):
    df = pd.DataFrame()
    page_number = 1
    flag = True
    while flag:
        try:
            url = f"{base_url}/api/v1/{resource}?page={page_number}"
            res = requests.get(url, headers=headers)
            if res.status_code != 200:
                logging.error(
                    f"Failed to fetch data for {resource} on page {page_number}: {res.status_code}"
                )
                break

            response_dict = json.loads(res.text)
            temp = pd.DataFrame(response_dict["items"])
            if temp.shape[0] == 0:
                flag = False
            else:
                df = pd.concat([df, temp], ignore_index=True)
                page_number += 1
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON response: {e}")
            flag = False
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            flag = False

    return df


if __name__ == "__main__":
    resources = [
        "subscriptions",
        "transactions",
        "users",
    ]  # Replace with actual resource names
    for resource in resources:
        data = download_dataframe_no_pagelimit(resource)
        if not data.empty:
            print(f"Data for resource {resource}:")
            print("Data headers:", data.columns.tolist())
            print("First few rows of data:")
            print(data.head())
        else:
            logging.info(f"No data retrieved for resource {resource}.")
