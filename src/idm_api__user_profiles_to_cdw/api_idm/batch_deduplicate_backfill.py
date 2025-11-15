'''import requires'''
import datetime
import logging
import time
import traceback

import pandas as pd
import sqlalchemy

from .get_token_users import get_users


def daterange(start_date, end_date):
    ''' get the range of dates '''
    for n in range(int((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

def save_progress(last_date, last_index, count, num_iterations):
    ''' Save progress in a text file '''
    logging.info("Saving progress")
    with open("progress_log.txt", "w", encoding='utf-8') as f:
        f.write(f"{last_date}\n{last_index}\n{count}\n{num_iterations}")

def load_progress():
    ''' Load progress from a text file '''
    try:
        with open("progress_log.txt", "r", encoding='utf-8') as f:
            contents = f.read()
            last_date, last_index, count, num_iterations = contents.split('\n')
            return last_date, int(last_index), int(count), int(num_iterations)
    except FileNotFoundError:
        logging.info("No progress file found")
        return None, 0, 100, 0
    except ValueError:
        logging.info(f"Error parsing progress file. Contents: '{contents}'")
        # Handle cases where the file exists but is empty or has less than 4 values
        return None, 0, 100, 0

def batch_get_users(api_url, username, password, apiauth, start_datetime, end_datetime):
    ''' Get users in batches '''
    logging.info("[batch_get_users] Entered function.")

    # Load progress from a text file
    last_date, last_index, prev_count, num_iterations = load_progress()

    # Convert last_date from string to datetime object, if it's not None
    if last_date is not None:
        last_date = datetime.datetime.strptime(last_date.replace('Z', '+00:00'), '%Y-%m-%dT%H:%M:%S%z')

    count = prev_count  # Here, you can set a new value to `count`

    logging.info("Last date: %s, Last index: %s, Previous count: %s, Number of iterations: %s", last_date, last_index, prev_count, num_iterations)

    if last_date is None:
        # This is the first run
        start_date = datetime.datetime.strptime(start_datetime.replace('Z', '+00:00'), '%Y-%m-%dT%H:%M:%S%z')
    else:
        # Resume from the last run
        start_date = last_date

    end_date = datetime.datetime.strptime(end_datetime.replace('Z', '+00:00'), '%Y-%m-%dT%H:%M:%S%z')

    responses = []

    try:
        for single_date in daterange(start_date, end_date):
            logging.info("[batch_get_users] Iterating over date: %s", single_date)
            date_str = single_date.strftime('%Y-%m-%dT%H:%M:%SZ')

            # Compare the last_date from the progress file with the current single_date
            if last_date is not None and last_date.date() == single_date.date():
                start_index = last_index
            else:
                start_index = 0

            # Apply filter for each day
            filters = f'''filter=meta.lastModified ge "{date_str}" \
                and meta.lastModified lt "{(single_date+datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')}"'''

            query_params = f'''?count={count}&startIndex={start_index}&{filters}'''

            logging.info("[batch_get_users] About to sleep.")
            time.sleep(1)
            logging.info("[batch_get_users] Finished sleeping.")

            logging.info("[batch_get_users] Sending request...")
            response = get_users(api_url,query_params,username,password,apiauth)
            logging.info("[batch_get_users] Received response.")

            if response is None:
                logging.warning("[batch_get_users] Empty response received, skipping to next iteration.")
                continue

            logging.info("[batch_get_users] start index: %s total results: %s count: %s number of responses: %s",
                        start_index,
                        response['totalResults'],
                        count,
                        len(response['Resources'])
                        )

            total_results = int(response['totalResults'])

            while len(responses) < total_results:
                logging.info("[batch_get_users] Inside while loop. Current responses length: %s, total_results: %s", len(responses), total_results)
                responses += response['Resources']

                if len(responses) >= 1000:
                    yield {'data': responses, 'date': date_str, 'start_index': start_index, 'count': count, 'num_iterations': num_iterations}
                    responses = []

                if len(response['Resources']) < count:
                    save_progress((single_date + datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'), 0, count, num_iterations)
                    break

                start_index += count
                query_params = f'''?count={count}&startIndex={start_index}&{filters}'''

                logging.info("[batch_get_users] About to sleep.")
                time.sleep(1)
                logging.info("[batch_get_users] Finished sleeping.")

                logging.info("[batch_get_users] Sending request...")
                response = get_users(api_url,query_params,username,password,apiauth)
                logging.info("[batch_get_users] Received response.")

                if response is None:
                    logging.warning("[batch_get_users] Empty response received, skipping to next iteration.")
                    break

                logging.info("[batch_get_users] start index: %s total results: %s count: %s number of responses: %s",
                            start_index,
                            response['totalResults'],
                            count,
                            len(response['Resources'])
                            )

                num_iterations += 1

            if responses:
                yield {'data': responses, 'date': date_str, 'start_index': start_index, 'count': count, 'num_iterations': num_iterations}

    except Exception as error:
        logging.error("[batch_get_users] %s", error)
        logging.error("Exception occurred", exc_info=True)  # this will log the full traceback

    logging.info("[batch_get_users] Exiting function.")



def remove_duplicates(bad_data):
    ''' remove the duplicates'''
    # handle empty list
    if not bad_data:
        return []

    # handle data is None
    if bad_data is None:
        return []

    # Remove any records that don't have an "id" key
    user_records = [record for record in bad_data if "id" in record]
    grouped_records = []
    for record in user_records:
        user_id = record["id"]
        matching_records = [r for r in grouped_records if r["id"] == user_id]
        if matching_records:
            latest_record = max(matching_records, key=lambda r: r["meta"]["lastModified"])
            if record["meta"]["lastModified"] > latest_record["meta"]["lastModified"]:
                grouped_records.remove(latest_record)
                grouped_records.append(record)
        else:
            grouped_records.append(record)
    # return grouped and sorted list
    return sorted(grouped_records, key=lambda r: r["id"])


def write_and_merge_data(data: pd.DataFrame, engine: sqlalchemy.engine.Engine, schema_name: str = 'idm'):
    ''' write and merge data '''

    sqlinsert = '''
        INSERT INTO idm.drupal__user_profiles (
             user_id, adobe_id, subscriber_id, username, record_source,
             active, country, postcode, street_address, display_name,
             emails_primary, emails_type, email, resource_type, last_name,
             first_name, phone_type, contact_phone,
             timezone, user_consent, email_verified, gender,
             mobile_verified, date_of_birth, created_date, last_modified,
             verified_date, mobile_verified_date, year_of_birth,
             city_region,
             locality,
             record_load_dts_utc,
             hash_diff,
             newsletter_subs,
             marketing_id,
             marketing_id_email
             )
        SELECT
             user_id, adobe_id, subscriber_id, username, record_source,
             active, country, postcode, street_address, display_name,
             emails_primary, emails_type, email, resource_type, last_name,
             first_name, phone_type, contact_phone,
             timezone, user_consent, email_verified, gender,
             mobile_verified, date_of_birth, created_date, last_modified,
             verified_date, mobile_verified_date, year_of_birth,
             city_region,
             locality,
             record_load_dts_utc,
             hash_diff,
             newsletter_subs,
             marketing_id,
             marketing_id_email
        FROM idm.temp_drupal__user_profiles
        WHERE user_id NOT IN (SELECT user_id FROM idm.drupal__user_profiles);
    '''
    sqlupdate = '''
        UPDATE idm.drupal__user_profiles
        SET
            adobe_id = s.adobe_id,
            subscriber_id = s.subscriber_id,
            username = s.username,
            record_source = s.record_source,
            active = s.active,
            country = s.country,
            postcode = s.postcode,
            street_address = s.street_address,
            display_name = s.display_name,
            emails_primary = s.emails_primary,
            emails_type = s.emails_type,
            email = s.email,
            resource_type = s.resource_type,
            last_name = s.last_name,
            first_name = s.first_name,
            phone_type = s.phone_type,
            contact_phone = s.contact_phone,
            timezone = s.timezone,
            user_consent = s.user_consent,
            email_verified = s.email_verified,
            gender = s.gender,
            mobile_verified = s.mobile_verified,
            date_of_birth = s.date_of_birth,
            created_date = s.created_date,
            last_modified = s.last_modified,
            verified_date = s.verified_date,
            mobile_verified_date = s.mobile_verified_date,
            year_of_birth = s.year_of_birth,
            city_region = s.city_region,
            locality = s.locality,
            record_load_dts_utc = s.record_load_dts_utc,
            hash_diff = s.hash_diff,
            newsletter_subs = s.newsletter_subs,
            marketing_id = s.marketing_id,
            marketing_id_email = s.marketing_id_email
        FROM idm.temp_drupal__user_profiles s
        WHERE drupal__user_profiles.user_id = s.user_id;
    '''

    try:
        logging.info("writing data to cdw temp table temp_drupal__user_profiles")
        data.to_sql('temp_drupal__user_profiles',
                        engine,
                        schema=schema_name,
                        if_exists='replace',
                        index=False,
                        index_label='user_id',
                        # order the columns
                        # get the data types from the schema definition in SCHEMA_DRUPAL__USER_PROFILES
                        dtype={
                            'user_id': sqlalchemy.types.Integer(),
                            'adobe_id': sqlalchemy.types.String(255),
                            'active': sqlalchemy.types.String(255),
                            'country': sqlalchemy.types.String(255),
                            'postcode': sqlalchemy.types.String(255),
                            'street_address': sqlalchemy.types.String(255),
                            'display_name': sqlalchemy.types.String(255),
                            'emails_primary': sqlalchemy.types.String(255),
                            'emails_type': sqlalchemy.types.String(255),
                            'email': sqlalchemy.types.String(255),
                            'created_date': sqlalchemy.types.DateTime(timezone=True),
                            'last_modified': sqlalchemy.types.DateTime(timezone=True),
                            'resource_type': sqlalchemy.types.String(255),
                            'last_name': sqlalchemy.types.String(255),
                            'first_name': sqlalchemy.types.String(255),
                            'phone_type': sqlalchemy.types.String(255),
                            'contact_phone': sqlalchemy.types.String(255),
                            'timezone': sqlalchemy.types.String(255),
                            'user_consent': sqlalchemy.types.String(255),
                            'date_of_birth': sqlalchemy.types.DateTime(),
                            'email_verified': sqlalchemy.types.String(255), # changed from Integer for csv
                            'verified_date': sqlalchemy.types.DateTime(timezone=True),
                            'gender': sqlalchemy.types.String(255),
                            'mobile_verified': sqlalchemy.types.String(255), # changed from Integer for csv
                            'mobile_verified_date': sqlalchemy.types.DateTime(timezone=True),
                            'subscriber_id': sqlalchemy.types.String(255),
                            'year_of_birth': sqlalchemy.types.DateTime(),
                            'username': sqlalchemy.types.String(255),
                            'record_source': sqlalchemy.types.String(255),
                            'city_region': sqlalchemy.types.String(255),
                            'locality': sqlalchemy.types.String(255),
                            'record_load_dts_utc': sqlalchemy.types.DateTime(),
                            'hash_diff': sqlalchemy.types.String(255),
                            'newsletter_subs': sqlalchemy.types.UnicodeText,
                            'marketing_id': sqlalchemy.types.String(36),
                            'marketing_id_email': sqlalchemy.types.String(36)
                            },
                            chunksize=1000
                    )
    except Exception as error:
        logging.info("Error with writing to temp table: %s", str(error))

    try:
        with engine.connect() as conn:
            logging.info("inserting new data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles")
            conn.execute(sqlinsert)
            logging.info("merging/updating data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles")
            conn.execute(sqlupdate)
        with engine.connect() as conn:
            logging.info("dropping temp table idm.temp_drupal__user_profiles")
            conn.execute('DROP TABLE IF EXISTS idm.temp_drupal__user_profiles;')
    except Exception as error:
        logging.info(error)
        logging.info('Error in merge')
