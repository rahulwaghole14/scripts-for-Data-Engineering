import json
import logging

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


# Custom HTTPAdapter to log retries
class LoggingHTTPAdapter(HTTPAdapter):
    def send(self, request, **kwargs):
        retries = kwargs.get('retries')
        if retries:
            logging.info(f"Retry attempt: {retries.total_retries - retries.total - 1}")
        return super().send(request, **kwargs)

# Configure logging
# logging.basicConfig(level=logging.INFO)

# Decorator for logging retries
def log_retries(func):
    def wrapper(*args, **kwargs):
        article_id = kwargs.pop("article_id", "Unknown")
        max_retries = kwargs.pop('max_retries', 5)
        retries = 0
        while retries <= max_retries:
            result = func(*args, **kwargs)
            if result is not None and result.status_code == 200:
                return result
            logging.info(f"Retrying for article_id {article_id}... Attempt {retries + 1}")
            retries += 1
        return None
    return wrapper

def init_session():
    ''' http session setup '''
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504]
    )
    adapter = LoggingHTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.get = log_retries(session.get)
    return session

def get_articles(article_id, apiurl, session):
    '''get articles into pandas dataframe'''
    url = f"{apiurl}{article_id}"
    try:
        response = session.get(url, timeout=20, article_id=article_id)
        if response is None:
            logging.error(f"Response is None for article_id {article_id}.")
            return None
        response.raise_for_status()
        return response
    except requests.HTTPError as error:
        if error.response.status_code == 404:
            logging.warning("404: Article id %s not found.", article_id)
        else:
            logging.error("HTTPError %s for article id %s.", error.response.status_code, article_id)
        return None
    except requests.RequestException as error:
        logging.error("RequestException for article id %s: %s", article_id, error)
        return None

def get_articles_list(url, session):
    '''get articles to dataframe'''
    assets = []
    try:
        response = session.get(url, timeout=20)
        response.raise_for_status()
        assets = [asset['assetId'] for asset in response.json()['queryResults']]

        while response.json()['paging']['next'] is not None:
            logging.info("Getting next page of results for url: %s", url)
            response = session.get(response.json()['paging']['next'], timeout=20)
            assets.extend([asset['assetId'] for asset in response.json()['queryResults']])

        return assets
    except requests.RequestException as error:
        logging.info("Request Exception occurred for asset: %s.", assets['assetId'])
        return None

def get_mapped_article_data(response, article_id):
    ''' get article data '''

    if response is None:
        return None

    try:
        # Parse the JSON response properly
        article_data = response.json()
        categories = article_data.get('brandsAndCategories')[0].get('categories')
        first_category = categories[0].get('category')
        categories_list = first_category.split('/')

        data = {
            'content_id': article_data.get('assetId'),
            'title': article_data.get('headline'),
            'author': article_data.get('byline'),
            'published_dts': article_data.get('firstPublishTime'),
            'source': article_data.get('sources')[0].get('source'),
            'brand': article_data.get('brandsAndCategories')[0].get('brands')[0].get('brand'),
            'category': article_data.get('brandsAndCategories')[0].get('categories')[0].get('category'),
            'word_count': article_data.get('currentWordCount'),
            'print_slug': article_data.get('printSlug'),
            'advertisement': article_data.get('advertisement'),
            'sponsored': article_data.get('sponsored'),
            'promoted_flag': article_data.get('promotedBrand'),
            'comments_flag': article_data.get('allowComments'),
            'home_flag': 1 if article_data.get('printSlug') == 'nz/hexa-homepage' else 0,
            'image_count': len([asset for asset in article_data.get('relatedAssets') if asset.get('type') == 'RelatedImage']),
            'video_count': len([asset for asset in article_data.get('relatedAssets') if asset.get('type') == 'RelatedVideo']),
        }

        # Add categories to the data dictionary
        for i in range(6):
            key = f'category_{i+1}'
            value = categories_list[i] if len(categories_list) > i else None
            data[key] = value
    except json.decoder.JSONDecodeError as error:
        logging.info(f"article: {article_id} | JSON Decode Error occurred for response: {response}... {response.content}... {error} Skipping...")
        data = None
        # raise SystemExit(error) from error
    return data

def get_articles_to_dataframe(data, apiurl, session):
    ''' get articles to dataframe '''
    # get all data for all articles and add to dataframe
    data_frame = pd.DataFrame()

    count = 0
    for asset_id in data:
        # print got X articles each 10 got:
        if count % 20 == 0:
            logging.info("got " + str(count) + " articles...")
        # get article data
        article_data = get_mapped_article_data(get_articles(asset_id, apiurl, session), asset_id)
        # add article data to dataframe if not none
        if article_data is not None:
            # data_frame = data_frame.append(article_data, ignore_index=True) # deprecated
            data_frame = pd.concat([data_frame, pd.DataFrame([article_data])], ignore_index=True)
        # only get first 100 articles else break
        # if count == 100:
        #     break
        count += 1

    return data_frame
