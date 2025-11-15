''' test clean data functions '''
import pandas as pd
import logging
from .clean_data import replace_non_utf8, read_list_of_dicts, remove_duplicates, map_columns, convert_json_string, get_value_from_dict
from .schema import SCHEMA_DRUPAL__USER_PROFILES

def test_read_list_of_dicts():
    ''' test read list of dicts '''
    list_of_dicts = [
        {'id': '1', 'name': {'givenName': 'John', 'familyName': 'Doe'}}
      , {'id': '2', 'name': {'givenName': 'Jane', 'familyName': 'Doe'}}
      , {'id': '3', 'name': {'givenName': 'John', 'familyName': 'Smith'}}
      , {'id': '4', 'name': {'givenName': 'Jane', 'familyName': 'Smith'}}
    ]
    dataframe = read_list_of_dicts(list_of_dicts)
    assert dataframe.shape == (4, 2)
    assert dataframe['id'].tolist() == ['1', '2', '3', '4']

# def test_empty_columns():
#     # if dataframe columns are empty, map_columns should still work and return None into the new column
#     list_of_dicts = [
#           {'id': '1'}
#         , {'id': '2'}
#         , {'id': '3'}
#         , {'id': '4'}
#         , {'coolguy': 'david'}
#     ]
#     dataframe = read_list_of_dicts(list_of_dicts)
#     dataframe['emails'] = None
#     logging.info(dataframe)
#     dataframe = map_columns(dataframe)

#     # the number of keys in SCHEMA_DRUPAL__USER_PROFILES should match the number of columns in the dataframe
#     assert len(SCHEMA_DRUPAL__USER_PROFILES.keys()) == len(dataframe.columns)
#     assert dataframe['user_id'].tolist() == [1, 2, 3, 4]
#     assert dataframe['email'].tolist() == [None, None, None, None]
#     assert dataframe['adobe_id'].tolist() == [None, None, None, None]
#     assert dataframe['emails_primary'].tolist() == [None, None, None, None]
#     assert dataframe['emails_type'].tolist() == [None, None, None, None]
#     assert dataframe['email'].tolist() == [None, None, None, None]
#     # test column order is correct compare to schema in SCHEMA_DRUPAL__USER_PROFILES
#     assert dataframe.columns.tolist() == [str(key) for key in SCHEMA_DRUPAL__USER_PROFILES.keys()]


def test_replace_non_utf8():
    '''test replace non utf8'''
    dataframe = pd.DataFrame({'col1': [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k'
      , 'l', 'm', 'n', 'o'
      , 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
      , ' ', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')'
      , '-', '_', '=', '+', '[', ']', '{', '}', '|', ';', ':'
      , "'", '"', ',', '.', '/', '?', 'á', 'é', 'í', 'ó', 'ú'
      , 'ñ', 'Á', 'É', 'Í', 'Ó', 'Ú', 'Ñ', 'à', 'è', 'ì', 'ò'
      , 'ù', 'À', 'È', 'Ì', 'Ò', 'Ù', 'â', 'ê', 'î', 'ô', 'û'
      , 'Â', 'Ê', 'Î', 'Ô', 'Û', 'ä', 'ë', 'ï', 'ö', 'ü', 'Ä'
      , 'Ë', 'Ï', 'Ö', 'Ü', 'ã', 'õ', 'Ã', 'Õ', 'å', 'Å', 'æ'
      , 'Æ', 'ç', 'Ç', 'ð', 'Ð', 'ø', 'Ø', 'ß', 'þ', 'Þ'
      , 'ý', 'Ý', 'ÿ', 'Ÿ', 'œ', 'Œ', '€', '£', '¥', '©', '®'
      , '™', '¡', '¿', '¡', '¿', '«', '»', '‹', '›', '“', '”'
      , '‘', '’', '–', '—', '…', '‒', '′', '″', '‴', '⁄', '⁒'
      , '⁓', '⁰', 'ⁱ', '⁴', '⁵', '⁶', '⁷', '⁸', '⁹', '⁺', '⁻'
      , '⁼', '⁽', '⁾',
    ]})
    dataframe = replace_non_utf8(dataframe)
    assert dataframe['col1'].str.contains(r'[^\x00-\x7F]+').sum() == 0

def test_remove_duplicates():
    ''' test remove duplicates '''
    dataframe = pd.DataFrame({'user_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
                              'last_modified': ['2'] * 10
                              })
    dataframe = remove_duplicates(dataframe)
    assert dataframe.shape == (10, 2)
    dataframe = pd.DataFrame({'user_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '10'],
                              'last_modified': ['2'] * 11})
    dataframe = remove_duplicates(dataframe)
    assert dataframe.shape == (10, 2)
    dataframe = pd.DataFrame({'user_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '10', '10'],
                              'last_modified': ['2'] * 12})
    dataframe = remove_duplicates(dataframe)
    assert dataframe.shape == (10, 2)

# def test_map_columns():
#     ''' test map columns '''
#     dataframe = pd.DataFrame()
#     dataframe['id'] = ['123', '456']
#     dataframe['externalId'] = ['123', '456']
#     dataframe['active'] = [True, True]
#     dataframe['userName'] = ['test1', 'test2']
#     dataframe['displayName'] = ['test1', 'test2']
#     dataframe['addresses'] = [{'country': 'US', 'postalCode': '12345', 'primary': '123 Main St'}, {'country': 'US', 'postalCode': '12345', 'primary': '123 Main St'}]
#     dataframe['emails'] = [{'type': 'work', 'value': 'email@email.com'}, {'type': 'work', 'value': 'email2@email.com'}]
#     dataframe['meta'] = [{'created': '2020-01-01T00:00:00', 'lastModified': '2020-01-01T00:00:00', 'resourceType': 'User'}, {'created': '2020-01-01T00:00:00', 'lastModified': '2020-01-01T00:00:00', 'resourceType': 'User'}]
#     dataframe['name'] = [{'familyName': 'last', 'givenName': 'first'}, {'familyName': 'last2', 'givenName': 'first2'}]
#     dataframe['phoneNumbers'] = [{'type': 'work', 'value': '1234567890'}, {'type': 'work', 'value': '1234567890'}]
#     dataframe['roles'] = [['role1', 'role2'], ['role3', 'role4']]
#     dataframe['schemas'] = [['urn:ietf:params:scim:schemas:core:2.0:User', 'urn:ietf:params:scim:schemas:extension:custom:2.0:User'], ['urn:ietf:params:scim:schemas:core:2.0:User', 'urn:ietf:params:scim:schemas:extension:custom:2.0:User']]
#     dataframe['timezone'] = ['America/New_York', 'America/New_York']
#     dataframe['urn:ietf:params:scim:schemas:extension:custom:2.0:User'] = [{'custom:custom1': 'custom1', 'custom:custom2': 'custom2'}, {'custom:custom1': 'custom1', 'custom:custom2': 'custom2'}]
#     dataframe['date_of_birth'] = ['2020-01-01', '2020-01-01']
#     dataframe['email_verified'] = [True, False]
#     dataframe['verified_date'] = ['2020-01-01T00:00:00', '2020-01-01T00:00:00']
#     dataframe['gender'] = ['male','female']
#     dataframe['mobile_verified'] = [True, False]
#     dataframe['mobile_verified_date'] = ['2020-01-01T00:00:00', '2020-01-01T00:00:00']
#     dataframe['subscriber_id'] = ['123', '456']
#     dataframe['year_of_birth'] = ['2020-01-01', '2020-01-01']
#     dataframe['record_load_dts_utc'] = ['2020-01-01T00:00:00', '2020-01-01T00:00:00']

#     dataframe = map_columns(dataframe)
#     assert dataframe.shape == (2, len(list(SCHEMA_DRUPAL__USER_PROFILES.keys())))
#     assert dataframe.columns.tolist() == list(SCHEMA_DRUPAL__USER_PROFILES.keys())

def test_convert_json_string():
    ''' test convert json string '''
    string = '{"test": "test"}'
    string2 = '{"test": None}'
    string3 = "{'test': 'value'}"
    # error data this should return None
    string4 = '"test": "test"}'
    assert convert_json_string(string) == {'test': 'test'}
    assert convert_json_string(string2) == {'test': 'None'}
    assert convert_json_string(string3) == {'test': 'value'}
    assert convert_json_string(string4) is None

def test_get_value_from_dict():
    ''' Test getting value from dictionary '''
    dictitem = {'name': 'John', 'age': 30}
    key = 'name'
    assert get_value_from_dict(dictitem, key) == 'John'

    # Test getting value from nested dictionary
    dictitem = {'name': 'John', 'age': 30, 'contact': {'email': 'john@example.com', 'phone': '123-456-7890'}}
    key = 'email'
    assert get_value_from_dict(dictitem, key) == 'john@example.com'

    # Test key not in dictionary
    dictitem = {'name': 'John', 'age': 30}
    key = 'email'
    assert get_value_from_dict(dictitem, key) is None

    # Test non-dictionary input
    dictitem = ['John', 30]
    key = 'name'
    assert get_value_from_dict(dictitem, key) is None
