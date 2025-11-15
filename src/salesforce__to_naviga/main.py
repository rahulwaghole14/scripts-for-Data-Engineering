''' salesforce data migration to naviga '''
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from logger import logger
from simple_salesforce import Salesforce

pd.set_option('display.max_colwidth', None)

# Load the environment variables from .env file
logger('salesforce')
load_dotenv()
date_time = time.strftime("%Y%m%d-%H%M%S")
objects = ['Account', 'Contact', 'Opportunity', 'User']
SAVE_CSVS = True
# use local or reload data from Salesforce, set to false
USE_LOCAL_FILE = True
# genera file
FILE2 = 'google_drive/GeneraCustomers20230421.csv'
# sales rep ids
FILE = 'google_drive/Sales_Rep_Export__RadGridExport.csv'

def map_countries_to_iso(billing_countries, shipping_countries):
    ''' map random country names to the 3 digit iso '''
    codes = {
        "New Zealand": "NZL",
        "NEW ZEALAND": "NZL",
        "Australia": "AUS",
        "AUSTRALIA": "AUS",
        "United States": "USA",
        "USA": "USA",
        "Hong Kong": "HKG",
        "United Kingdom": "GBR",
        "Fiji": "FJI",
        "NZ": "NZL",
        "AUS": "AUS",
        "America": "USA",
        "Channel Islands": "GGY",  # Using Guernsey's ISO code as an example
        "AU": "AUS",
        "British Virgin Islands": "VGB",
        "UK": "GBR",
        "Palmerston North": "NZL",
        "New Zealan": "NZL",
        "New York": "USA",
        "Wellington": "NZL",
        "Rarotonga": "COK",  # Cook Islands
        "nz": "NZL",
        "New Zaland": "NZL",
        "United Sates of America": "USA",
        "New ZeTaland": "NZL",
        "Cook Islands": "COK",
        "New ZEaland": "NZL",
        "New zealand": "NZL"
    }

    result = []
    for billing, shipping in zip(billing_countries, shipping_countries):
        code = codes.get(billing)
        if code is None:
            code = codes.get(shipping, 'NZL')
        result.append(code)
    return result

def find_duplicates(dataframe):
    ''' produce duplicate record csvs '''
    fields_dup = [
        'Legal_Name__c',
        'Genera_Account_ID__c',
        'Genera_Account_Number__c',
        'SAP_Account_Number__c'
        ]
    for field in fields_dup:
        # Filter out rows with None values in the current field
        dataframe = dataframe[dataframe[field].notnull()]

        duplicated_rows = dataframe[dataframe.duplicated(subset=field, keep=False)]
        duplicated_rows = duplicated_rows.dropna(subset=[field])
        duplicated_rows = duplicated_rows.sort_values(by=[field])
        duplicated_rows = duplicated_rows[[field] + list(duplicated_rows.columns)[:-1]]
        filename = str.lower(field)

        # filter to column field and Id_x field but renamed to AccountId
        duplicated_rows = duplicated_rows[[field, 'Id_x']]
        duplicated_rows.to_csv(
            f"naviga_output/duplicates/{date_time}_salesforce__duplicate_{filename}.csv",
            index=False,
            encoding='utf-8'
            )

fields_gen = [
    'Description',
    'Genera_Account_Error__c',
    'ShippingStreet',
    'BillingStreet',
    'ShippingAddress.street',
    'BillingAddress.street'
    ]

fields_opportunity = [
    'Description',
    'AB2__AB_Campaign_Notes__c',
    'LostReason__c',
    'Bonus_Comments__c',
    'ErrorLog__c',
    'AB2__ABRateCard__c',
    'Distress_Comments__c'
    ]

fields_contact = [
    'MailingStreet',
    'OtherStreet',
    'MailingAddress.street',
    'OtherAddress.street',
    'Description'
    ]

def tidy_strings(messy_data_frame, fieldlist):
    ''' remove line breaks in bad strings '''
    for field in fieldlist:
        messy_data_frame[field] = messy_data_frame[field].str.replace('\r', ' ')
        messy_data_frame[field] = messy_data_frame[field].str.replace('\n', ' ')

    return messy_data_frame


if USE_LOCAL_FILE is False:

    try:
        # Set params from environment variable
        sf_username = os.environ.get("sf_username")
        sf_password = os.environ.get("sf_password")
        sf_security_token = os.environ.get("sf_security_token")
        sf_url = os.environ.get("sf_url")

        # Authenticate
        sf = Salesforce(
            instance_url=sf_url,
            username=sf_username,
            password=sf_password,
            security_token=sf_security_token
        )
        logging.info("Successfully authenticated to Salesforce")
    except Exception as Error:
        logging.info("Error: %s", str(Error))

    try:

        for i in objects:
            OBJECT_NAME = i
            logging.info("Getting metadata for object %s", OBJECT_NAME)
            if OBJECT_NAME == 'User':
                metadata = sf.User.describe()
            if OBJECT_NAME == 'Account':
                metadata = sf.Account.describe()
            if OBJECT_NAME == 'Contact':
                metadata = sf.Contact.describe()
            if OBJECT_NAME == 'Opportunity':
                metadata = sf.Opportunity.describe()
            fields = [field['name'] for field in metadata['fields']]
            soql_query = f"SELECT {', '.join(fields)} FROM {OBJECT_NAME}"
            logging.info("Executing query on object %s", OBJECT_NAME)
            query_result = sf.query_all(soql_query)
            data_frame = pd.json_normalize(query_result['records'])
            data_frame.to_parquet(f'local_storage/salesforce__{OBJECT_NAME}.parquet', index=False)
            logging.info(
                "Successfully saved data_frame for %s to local_storage/salesforce__%s.parquet",
                OBJECT_NAME,
                OBJECT_NAME
                )

        logging.info("Successfully retrieved all data from Salesforce")
    except Exception as Error:
        logging.info("Error: %s", str(Error))

### all tables
# load data from local_storage
data_frame = pd.read_parquet('local_storage/salesforce__account.parquet')
# print columns that contain 'Address'
logging.info("Columns that contain 'Address'")
logging.info(data_frame.columns[data_frame.columns.str.contains('Address')])
data_frame_user = pd.read_parquet('local_storage/salesforce__user.parquet')
data_frame_contact = pd.read_parquet('local_storage/salesforce__contact.parquet')
data_frame_opportunity = pd.read_parquet('local_storage/salesforce__opportunity.parquet')

# count number of records in raw files
logging.info("Number of records in raw files")
logging.info("Account: %s", len(data_frame))
logging.info("User: %s", len(data_frame_user))
logging.info("Contact: %s", len(data_frame_contact))
logging.info("Opportunity: %s", len(data_frame_opportunity))

# drop any columns that are empty
data_frame = data_frame.dropna(axis=1, how='all')
data_frame_user = data_frame_user.dropna(axis=1, how='all')
data_frame_contact = data_frame_contact.dropna(axis=1, how='all')
data_frame_opportunity = data_frame_opportunity.dropna(axis=1, how='all')

# ---------------------------------------- #
### accounts

# Drop rows if 'Name' or 'Description' contains 'DO NOT USE' (case-insensitive) and is not None
data_frame = data_frame[
    ~data_frame['Name'].apply(lambda x: x is not None and 'do not use' in x.lower()) &
    ~data_frame['Description'].apply(lambda x: x is not None and 'do not use' in x.lower())
]

# remove link breaks from \r\n string
data_frame = tidy_strings(data_frame, fields_gen)

# get owner, and sales info, genera merge
data_frame = data_frame.merge(data_frame_user, left_on='OwnerId', right_on='Id', how='left')
data_frame_sales_rep = pd.read_csv(FILE, encoding='utf-8')
data_frame = data_frame.merge(
    data_frame_sales_rep,
    left_on='Name_y',
    right_on='Sales Rep Name',
    how='left'
    )
data_frame_genera = pd.read_csv(FILE2, encoding='utf-8', low_memory=False)
data_frame['Legal_Name__c'] = data_frame['Legal_Name__c'].astype(str)
data_frame['SAP_Account_Number__c'] = data_frame['SAP_Account_Number__c'].astype(str)
data_frame['Genera_Account_Number__c'] = data_frame['Genera_Account_Number__c'].astype(str)
data_frame_genera['Name'] = data_frame_genera['Name'].astype(str)
data_frame_genera['Legacy Company ID'] = data_frame_genera['Legacy Company ID'].astype(str)
data_frame_genera['Legacy Company ID 2'] = data_frame_genera['Legacy Company ID 2'].astype(str)
data_frame = data_frame.merge(
    data_frame_genera,
    left_on=[
    'Legal_Name__c',
    'SAP_Account_Number__c',
    'Genera_Account_Number__c'
    ],
    right_on=['Name', 'Legacy Company ID', 'Legacy Company ID 2'],
    how='outer',
    indicator=True
)

data_frame['is_matched_genera'] = data_frame['_merge'].apply(
    lambda x: True if x == 'both' else False
    )
data_frame['is_not_matched_salesforce'] = data_frame['_merge'] == 'right_only'
data_frame.drop(columns=['_merge'], inplace=True)

# simple list of final account ids
data_frame['AccountId'] = data_frame['Id_x']
data_frame_accounts = data_frame[['AccountId']]

# ------------------------------------- #
### Opportunities filtering
# Merge dataframes using inner join
# (this must be done because some accounts are 'do not use' in the name)
data_frame_opportunity = data_frame_opportunity[
    data_frame_opportunity['AccountId'].isin(
    data_frame_accounts['AccountId'])]

# remove closed opportunities
data_frame_opportunity = data_frame_opportunity[
    data_frame_opportunity['StageName'] != 'Closed Won']
data_frame_opportunity = data_frame_opportunity[
    data_frame_opportunity['StageName'] != 'Closed Lost']

# remove old opportunities (closedate has to be 30 days before now and future)
data_frame_opportunity['CloseDate'] = pd.to_datetime(data_frame_opportunity['CloseDate'])
days_ago_30 = datetime.now().date() - timedelta(days=30)
data_frame_opportunity = data_frame_opportunity[
    data_frame_opportunity['CloseDate'].dt.date >= days_ago_30]

# remove duplicate opportunities
# clean data for bad strings
data_frame_opportunity = tidy_strings(data_frame_opportunity, fields_opportunity)


# ------------------------------------- #
### Accounts final filtering
# has d365/genera account or has opportunity in above list
# populate has_opportunity column
data_frame['has_opportunity'] = data_frame['AccountId'].isin(data_frame_opportunity['AccountId'])
# populate only genera data present

# filter is_matched_genera is True or has_opportunity is True
data_frame = data_frame[
    data_frame['is_matched_genera'] |
    data_frame['has_opportunity'] |
    data_frame['is_not_matched_salesforce']
    ]
# drop some fields not useful
data_frame = data_frame.drop(
    columns=[
    'PhotoUrl',
    'Statements__c',
    'Flag__c',
    'attributes.url_x',
    'attributes.url_y'
    ])

# ------------------------------------- #
### Contacts final filtering
data_frame_contact = data_frame_contact[
    data_frame_contact['AccountId'].isin(data_frame['AccountId'])]
data_frame_contact = tidy_strings(data_frame_contact, fields_contact)
# sort by AccountId
data_frame_contact = data_frame_contact.sort_values(by=['AccountId'])



# ---- #
# opportunities extra data

data_frame_opp_merge = data_frame[data_frame['is_matched_genera']]
data_frame_opp_merge = data_frame_opp_merge[[
    'AccountId',
    'Legal_Name__c',
    'SAP_Account_Number__c',
    'Genera_Account_Number__c'
    ]]
# change col names
cols = ['x_acct_id', 'x_name', 'x_sap_number', 'x_genera_number']
# renam cols
data_frame_opp_merge.columns = cols

data_frame_opportunity = data_frame_opportunity.merge(
    data_frame_opp_merge,
    left_on='AccountId',
    right_on='x_acct_id',
    how='left'
)

data_frame_user = data_frame_user[['Id','Name','Email']]
data_frame_user.columns = ['sf_rep_id','sf_rep_name','sf_rep_email']
data_frame_opportunity = data_frame_opportunity.merge(
    data_frame_user,
    left_on='OwnerId',
    right_on='sf_rep_id',
    how='left'
    )
data_frame_opportunity = data_frame_opportunity.merge(
    data_frame_sales_rep,
    left_on='sf_rep_name',
    right_on='Sales Rep Name',
    how='left'
)

# Group by 'accountid' and apply aggregation functions
data_frame_material_contacts = data_frame_opportunity.groupby('AccountId').agg(
    material_contact_count=('Material_Contact__c', 'count'),
    material_contact_ids=('Material_Contact__c', lambda x: list(x))
).reset_index()

# rename columns
data_frame_material_contacts.columns = [
    'm_accountid',
    'm_material_contact_count',
    'm_material_contact_ids'
    ]

# has count of one:
data_frame_material_contacts_one = data_frame_material_contacts[
    data_frame_material_contacts['m_material_contact_count'] == 1
    ].copy()

# Change to str not list
data_frame_material_contacts_one.loc[:, 'filtered_ids'] = data_frame_material_contacts_one['m_material_contact_ids'].apply(
    lambda x: ', '.join(id_ for id_ in x if id_ is not None)
)

# Convert to string
data_frame_material_contacts_one.loc[:, 'filtered_ids'] = data_frame_material_contacts_one['filtered_ids'].astype(str)
data_frame_material_contacts_one = data_frame_material_contacts_one[['m_accountid', 'filtered_ids']]
data_frame_material_contacts_one.columns = ['mone_accountid','mone_id']

data_frame_opportunity = data_frame_opportunity.merge(
    data_frame_material_contacts_one,
    left_on='AccountId',
    right_on='mone_accountid',
    how='left'
    )

# print(data_frame_opportunity.head(20))

data_frame_opportunity = data_frame_opportunity.merge(
    data_frame_material_contacts,
    left_on='AccountId',
    right_on='m_accountid',
    how='left'
    )

# print(data_frame_contact.columns)
data_frame_contact_material_info = data_frame_contact[[
    'Id',
    'LastName',
    'FirstName',
    'Salutation',
    'Name',
    'Phone',
    'Fax',
    'MobilePhone',
    'Email',
    'Title'
]]

data_frame_contact_material_info.columns = [
    'material_contact_id',
    'material_contact_last_name',
    'material_contact_first_name',
    'material_contact_salutation',
    'material_contact_name',
    'material_contact_phone',
    'material_contact_fax',
    'material_contact_mobile_phone',
    'material_contact_email',
    'material_contact_title'
]

data_frame = data_frame.merge(
    data_frame_material_contacts_one,
    left_on='Id_x',
    right_on='mone_accountid',
    how='left'
)

data_frame = data_frame.merge(
    data_frame_contact_material_info,
    left_on='mone_id',
    right_on='material_contact_id',
    how='left'
    )

# print(data_frame_material_contacts_one.head(20))
# print(data_frame_opportunity.columns)
data_frame_opportunity = data_frame_opportunity.merge(
    data_frame_contact_material_info,
    left_on='mone_id',
    right_on='material_contact_id',
    how='left'
    )

# is is_matched_genera true
data_frame_matched_genera = data_frame[data_frame['is_matched_genera'] == True]
data_frame_matched_genera = data_frame_matched_genera[[
    'Id_x',
    'Legal_Name__c',
    'Legacy Company ID',
    'Legacy Company ID 2'
    ]]

data_frame_matched_genera.columns = [
    'salesforce_accountid',
    'name',
    'legacy_company_id',
    'legacy_company_id_2'
]

# Assuming 'data_frame' is your DataFrame with 'BillingCountry' and 'ShippingCountry' columns
data_frame['naviga_country_id'] = map_countries_to_iso(data_frame['BillingCountry'], data_frame['ShippingCountry'])

# ------------------------------------- #
### reporting counts
logging.info(
    "Total accounts: %s",
    data_frame['AccountId'].count()
    )
logging.info(
    "Total accounts that match genera: %s",
    data_frame['is_matched_genera'].sum()
    )
logging.info(
    "Total accounts that have opportunities: %s",
    data_frame['AccountId'].isin(data_frame_opportunity['AccountId']).sum()
    )

# get owner
# print(data_frame_contact['OwnerId'].head(10))
# print(data_frame_user.columns)
# print(data_frame_user['sf_rep_id'].head(10))
data_frame_contact = data_frame_contact.merge(
    data_frame_user, left_on='OwnerId', right_on='sf_rep_id', how='left'
    )

data_frame_contact  = data_frame_contact.merge(
    data_frame_sales_rep, left_on='sf_rep_name', right_on='Sales Rep Name', how='left'
    )

if SAVE_CSVS is True:

    data_frame_matched_genera.to_csv(
        f"naviga_output/accounts/{date_time}_salesforce__accounts_match_list.csv",
        index=False,
        encoding='utf-8'
        )

    data_frame.to_csv(
        f"naviga_output/accounts/{date_time}_salesforce__accounts_to_naviga.csv",
        index=False,
        encoding='utf-8'
        )
    logging.info("Successfully saved data_frame to naviga_output/accounts/accounts_to_naviga.csv")


    # reduce columns
    # print columns that contain 'Address'
    logging.info("Reducing columns in data_frame_contact")
    logging.info(data_frame.columns[data_frame.columns.str.contains('Address')])

    data_frame = data_frame[[
        'Id_x',
        'Name_x',
        'Type',
        'naviga_country_id',
        'OwnerId',
        'Account_Type__c',
        'Genera_Account_ID__c',
        'Genera_Account_Number__c',
        'SAP_Account_Number__c',
        'Legal_Name__c',
        'Short_Name__c',
        'Name_y',
        'Sales Rep ID',
        'Sales Rep Name',
        'Email',
        'Primary Group',
        'Legacy Company ID',
        'is_matched_genera',
        'is_not_matched_salesforce',
        'has_opportunity',
        'material_contact_id',
        'material_contact_last_name',
        'material_contact_first_name',
        'material_contact_salutation',
        'material_contact_name',
        'material_contact_phone',
        'material_contact_fax',
        'material_contact_mobile_phone',
        'material_contact_email',
        'material_contact_title',
        'Billing_Address_Validated__c', 'Physical_Address_Validated__c',
       'Billing_Address_Territory__c', 'Physical_Address_Territory__c',
       'BillingAddress.city', 'BillingAddress.country',
       'BillingAddress.postalCode', 'BillingAddress.state',
       'BillingAddress.street', 'ShippingAddress.city',
       'ShippingAddress.country', 'ShippingAddress.latitude',
       'ShippingAddress.longitude', 'ShippingAddress.postalCode',
       'ShippingAddress.state', 'ShippingAddress.street', 'Address.city',
       'Address.country', 'Address.postalCode', 'Address.state',
       'Address.street', 'Address 1', 'Address 2', 'Address 3'
    ]]

    data_frame.to_csv(
        f"naviga_output/accounts/{date_time}_salesforce__accounts_reduced_columns_to_naviga.csv",
        index=False,
        encoding='utf-8'
        )

    data_frame_contact.to_csv(
        f"naviga_output/contacts/{date_time}_salesforce__contact.csv",
        index=False,
        encoding='utf-8'
        )
    logging.info("Successfully saved data_frame_contact to naviga_output/contacts/contact.csv")

    data_frame_opportunity.to_csv(
        f"naviga_output/opportunities/{date_time}_salesforce__opportunities_to_naviga.csv",
        index=False,
        encoding='utf-8'
    )

    # reduce columns in data_frame_opportunity
    #columns to keep
    # Id,AccountId,Name,StageName,Amount,Probability,ExpectedRevenue,TotalOpportunityQuantity,CloseDate,Type,Description,OwnerId,CreatedById,Channel__c,Customer_Category__c,Material_Contact__c,SubRegion__c,Sales_Team__c
    data_frame_opportunity = data_frame_opportunity[
        [
        'Id',
        'AccountId',
        'x_name',
        'x_sap_number',
        'x_genera_number',
        'Name',
        'StageName',
        'Amount',
        'Probability',
        'ExpectedRevenue',
        'TotalOpportunityQuantity',
        'CloseDate',
        'Type',
        'Description',
        'OwnerId',
        'CreatedById',
        'Channel__c',
        'Customer_Category__c',
        'Material_Contact__c',
        'SubRegion__c',
        'Sales_Team__c',
        'Sales Rep ID',
        'Sales Rep Name',
        'sf_rep_email',
        'Primary Group',
        'Outside Sales Rep',
        'Inactive',
        'm_material_contact_count',
        'm_material_contact_ids',
        'material_contact_id',
        'material_contact_last_name',
        'material_contact_first_name',
        'material_contact_salutation',
        'material_contact_name',
        'material_contact_phone',
        'material_contact_fax',
        'material_contact_mobile_phone',
        'material_contact_email',
        'material_contact_title'
        ]]

    # save data_frame_opportunity to csv
    data_frame_opportunity.to_csv(
        f"naviga_output/opportunities/{date_time}_salesforce__opportunities_reduced_columns_to_naviga.csv",
        index=False,
        encoding='utf-8'
    )

    logging.info(
        "Successfully saved data_frame_opportunity to \
        naviga_output/opportunities/opportunities_to_naviga.csv"
        )

    # write duplicate values to csvs in duplicates folder
    find_duplicates(data_frame)
    logging.info("Successfully wrote duplicate values to csvs in duplicates folder")

else:
    logging.info("SAVE_CSVS is set to False, skipping saving csvs")
