'''clean data'''
import datetime
import json
import ast
import logging
import pandas as pd
from .schema import SCHEMA_DRUPAL__USER_PROFILES
from .create_uuid import generate_uuid

def read_list_of_dicts(list_of_dicts):
    '''read list of dicts to dataframe'''
    dataframe = pd.DataFrame(list_of_dicts)
    # if empty primary key, drop row
    # if 'id' not in dataframe.columns:
    if 'id' not in dataframe.columns:
        dataframe['id'] = None
    dataframe = dataframe.dropna(subset=['id'])
    return dataframe

def replace_non_utf8(dataframe):
    '''replace non-utf8 characters with a replacement character'''
    dataframe = dataframe.replace(to_replace=r'[^\x00-\x7F]+', value=' ', regex=True)
    return dataframe

def get_value_from_dict(dictitem, key):
    '''get value from dict item recursively if key not found in dictitem return None'''
    if isinstance(dictitem, dict):
        if key in dictitem:
            return str(dictitem[key])
        else:
            for k, v in dictitem.items():
                if isinstance(v, dict):
                    result = get_value_from_dict(v, key)
                    if result is not None:
                        return result
            return None
    else:
        return None

def map_columns(dataframe, namespace):
    '''map columns to new schema'''

    try:
        # Map ids
        if 'externalId' not in dataframe.columns:
            dataframe['externalId'] = None
            dataframe['adobe_id'] = None
        else:
            dataframe['externalId'] = dataframe['externalId'].apply(lambda x: str(x) if isinstance(x, float) else x)
            dataframe['adobe_id'] = dataframe['externalId'] #.apply(lambda x: None if len(x) == 0 else x)

        if 'id' not in dataframe.columns:
            dataframe['id'] = None
            dataframe['user_id'] = None
        else:
            dataframe['user_id'] = dataframe['id'].astype('int')

        dataframe['record_source'] = 'IDM'

        if 'userName' not in dataframe.columns:
            dataframe['userName'] = None
            dataframe['username'] = None
        else:
            dataframe['username'] = dataframe['userName'] #.apply(lambda x: None if len(x) == 0 else x)

        if 'active' not in dataframe.columns:
            dataframe['active'] = None
        else:
            dataframe['active'] = dataframe['active'].astype('str')

        # Map addresses
        if 'addresses' not in dataframe.columns:
            dataframe['addresses'] = None
            dataframe['country'] = None
            dataframe['postcode'] = None
            dataframe['street_address'] = None
        else:
            dataframe['country'] = dataframe['addresses'].apply(lambda x: x[0]['country'] if isinstance(x, list) and len(x) > 0 and 'country' in x[0] else None)
            dataframe['postcode'] = dataframe['addresses'].apply(lambda x: x[0]['postalCode'] if isinstance(x, list) and len(x) > 0 and 'postalCode' in x[0] else None)
            dataframe['street_address'] = dataframe['addresses'].apply(lambda x: x[0]['primary'] if isinstance(x, list) and len(x) > 0 and 'primary' in x[0] else None)
            dataframe['street_address'] = dataframe['street_address'].astype(str)

        if 'displayName' not in dataframe.columns:
            dataframe['displayName'] = None
            dataframe['display_name'] = None
        else:
            dataframe['display_name']= dataframe['displayName'].replace(to_replace=r'[^\x00-\x7F]+', value=' ', regex=True)
            dataframe['display_name'] = dataframe['display_name'].apply(lambda x: x if x is not None else None)

        # Map email info
        if 'emails' not in dataframe.columns:
            dataframe['emails'] = None
            dataframe['emails_primary'] = None
            dataframe['emails_type'] = None
            dataframe['email'] = None
        else:
            dataframe['emails_primary'] = dataframe['emails'].apply(lambda x: x[0]['primary'] if isinstance(x, list) and len(x) > 0 and 'primary' in x[0] else None)
            dataframe['emails_primary'] = dataframe['emails_primary'].astype(str)
            dataframe['emails_type'] = dataframe['emails'].apply(lambda x: x[0]['type'] if isinstance(x, list) and len(x) > 0 and 'type' in x[0] else None)
            dataframe['email'] = dataframe['emails'].apply(lambda x: x[0]['value'] if isinstance(x, list) and len(x) > 0 and 'value' in x[0] else None)

        # Map meta info
        if 'meta' not in dataframe.columns:
            dataframe['meta'] = None
            dataframe['created_date'] = None
            dataframe['last_modified'] = None
            dataframe['resource_type'] = None
        else:
            dataframe['created_date'] = dataframe['meta'].apply(lambda x: get_value_from_dict(x, 'created'))
            dataframe['created_date'] = pd.to_datetime(dataframe['created_date'].str.strip(), errors='coerce')
            dataframe['created_date'] = dataframe['created_date'].replace({pd.NaT: None})

            dataframe['last_modified'] = dataframe['meta'].apply(lambda x: get_value_from_dict(x, 'lastModified'))
            dataframe['last_modified'] = pd.to_datetime(dataframe['last_modified'].str.strip(), errors='coerce')
            dataframe['last_modified'] = dataframe['last_modified'].replace({pd.NaT: None})
            dataframe['resource_type'] = dataframe['meta'].apply(lambda x: get_value_from_dict(x, 'resourceType'))

        # Map name info
        if 'name' not in dataframe.columns:
            dataframe['name'] = None
            dataframe['last_name'] = None
            dataframe['first_name'] = None
        else:
            dataframe['last_name'] = dataframe['name'].apply(lambda x: get_value_from_dict(x, 'familyName'))
            dataframe['first_name'] = dataframe['name'].apply(lambda x: get_value_from_dict(x, 'givenName'))

        if 'phoneNumbers' not in dataframe.columns:
            dataframe['phoneNumbers'] = None
            dataframe['phone_type'] = None
            dataframe['contact_phone'] = None
        else:
            dataframe['phone_type'] = dataframe['phoneNumbers'].apply(
                lambda x: x[0]['type'] if isinstance(x, list) and len(x) > 0 and 'type' in x[0]
                else None)
            dataframe['contact_phone'] = dataframe['phoneNumbers'].apply(
                lambda x: x[0]['value'] if isinstance(x, list) and len(x) > 0 and 'value' in x[0]
                else None)

        # if 'roles' not in dataframe.columns:
        #     dataframe['roles'] = None
        # dataframe['roles'] = dataframe['roles'].apply(
        #     lambda x: None if x is None
        #     else ','.join(map(str, x)))

        # if 'schemas' not in dataframe.columns:
        #     dataframe['schemas'] = None
        # dataframe['schemas'] = dataframe['schemas'].apply(lambda x: None if len(x) == 0 else ','.join(map(str, x)))

        if 'timezone' not in dataframe.columns:
            dataframe['timezone'] = None
        else:
            dataframe['timezone'] = dataframe['timezone']

        # get user schema data
        longcolumnname = 'urn:ietf:params:scim:schemas:extension:custom:2.0:User'
        if longcolumnname not in dataframe.columns:
            dataframe[longcolumnname] = None
            dataframe['user_consent'] = None
            dataframe['date_of_birth'] = None
            dataframe['email_verified'] = None
            dataframe['verified_date'] = None
            dataframe['gender'] = None
            dataframe['mobile_verified'] = None
            dataframe['mobile_verified_date'] = None
            dataframe['subscriber_id'] = None
            dataframe['year_of_birth'] = None
            dataframe['newsletter_sub_data'] = None
        else:

            dataframe['user_consent'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'consentReference'))
            dataframe['date_of_birth'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'dateOfBirth'))
            dataframe['date_of_birth'] = pd.to_datetime(dataframe['date_of_birth'].str.strip(), errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dataframe['date_of_birth'] = dataframe['date_of_birth'].replace({pd.NaT: None})
            dataframe['email_verified'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'emailVerified'))
            dataframe['verified_date'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'emailVerifiedDate'))
            dataframe['verified_date'] = pd.to_datetime(dataframe['verified_date'].str.strip(), errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dataframe['verified_date'] = dataframe['verified_date'].replace({pd.NaT: None})
            dataframe['gender'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'gender'))
            dataframe['mobile_verified'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'mobileNumberVerified'))
            dataframe['mobile_verified_date'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'mobileVerifiedDate'))
            dataframe['mobile_verified_date'] = pd.to_datetime(dataframe['mobile_verified_date'].str.strip(), errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dataframe['mobile_verified_date'] = dataframe['mobile_verified_date'].replace({pd.NaT: None})
            dataframe['subscriber_id'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'subscriberId'))
            dataframe['year_of_birth'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'yearOfBirth'))
            dataframe['year_of_birth'] = pd.to_datetime(dataframe['year_of_birth'].str.strip(), errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
            dataframe['year_of_birth'] = dataframe['year_of_birth'].replace({pd.NaT: None})
            dataframe['newsletter_subs'] = dataframe[longcolumnname].apply(lambda x: get_value_from_dict(x, 'newsletterSubscription'))
            dataframe['newsletter_subs'] = dataframe['newsletter_subs'].apply(lambda x: json.dumps(ast.literal_eval(x)) if x is not None else x)

        # determine old columns the api does not have
        dataframe['city_region'] = None
        dataframe['locality'] = None

        # determine record load time
        record_load_dts_utc = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        dataframe['record_load_dts_utc'] = record_load_dts_utc
        dataframe['hash_diff'] = None

        # generate uuid from hexa user_id
        dataframe['marketing_id'] = dataframe['user_id'].apply(lambda x: generate_uuid(x, namespace))
        dataframe['marketing_id_email'] = dataframe['email'].apply(lambda x: generate_uuid(x, namespace))

        # Drop columns that are not in the new schema
        for col in dataframe.columns:
            if col not in SCHEMA_DRUPAL__USER_PROFILES:
                dataframe.drop(col, axis=1, inplace=True)

        # Re-order columns to match the new schema
        ordered_columns = list(SCHEMA_DRUPAL__USER_PROFILES.keys())
        dataframe = dataframe[ordered_columns]
    except Exception as error:
        logging.info("error cleaning data: " + str(error))

    return dataframe

def remove_duplicates(dataframe):
    '''remove duplicates keep one with largest last_modified'''
    dataframe.sort_values(by=['last_modified'], ascending=False, inplace=True)
    dataframe.drop_duplicates(subset=['user_id'], keep='first', inplace=True)
    return dataframe

def convert_json_string(s):
    ''' convert strings to json from the csv '''
    try:
        parsed_list = s.replace("'", '"').replace("True", '"True"').replace("False", '"False"').replace("None", '"None"')
        parsed_list = json.loads(parsed_list)
        return parsed_list
    except json.JSONDecodeError:
        return None
